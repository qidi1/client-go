package tikv

import (
	"bytes"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/client"
	"github.com/tikv/client-go/v2/config"
	clientError "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/locate"
	"github.com/tikv/client-go/v2/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/retry"
	"github.com/tikv/client-go/v2/tikvrpc"
	zap "go.uber.org/zap"
)

// SplitKeyRangesByLocations splits the KeyRanges by logical info in the cache.
func SplitKeyRangesByLocations(bo *Backoffer, ranges []kv.KeyRange, regionCache *locate.RegionCache) ([]vertifyRange, error) {
	res := make([]vertifyRange, 0)
	for len(ranges) > 0 {
		loc, err := regionCache.LocateKey(bo, ranges[0].StartKey)
		if err != nil {
			return res, err
		}
		scanRange := vertifyRange{
			loc: loc,
		}
		// Iterate to the first range that is not complete in the region.
		var r kv.KeyRange
		var i int
		for ; i < len(ranges); i++ {
			r = ranges[i]
			if bytes.Compare(loc.EndKey, r.EndKey) >= 0 {
				scanRange.keyRange = r
				ranges = ranges[1:]
			} else {
				if loc.Contains(r.StartKey) {
					// region 没有包含全部 range
					scanRange.keyRange = kv.KeyRange{StartKey: r.StartKey, EndKey: loc.EndKey}
					ranges[i] = kv.KeyRange{StartKey: loc.EndKey, EndKey: r.EndKey}
				}
			}
		}
	}
	return res, nil
}

type vertifyRange struct {
	loc      *KeyLocation
	keyRange kv.KeyRange
}

func SortAndMergeRanges(ranges []kv.KeyRange) []kv.KeyRange {
	sort.Slice(ranges, func(i, j int) bool {
		a := bytes.Compare(ranges[i].StartKey, ranges[j].StartKey)
		if a < 0 {
			return true
		}
		if a > 0 {
			return false
		}
		return bytes.Compare(ranges[i].EndKey, ranges[j].EndKey) < 0
	})
	result := make([]kv.KeyRange, 0, len(ranges))
	rangeStartKey, rangeEndKey := ranges[0].StartKey, ranges[0].EndKey
	for i := 1; i < len(ranges); i++ {
		subStartKey, subEndKey := ranges[i].StartKey, ranges[i].EndKey
		if bytes.Compare(subStartKey, rangeEndKey) <= 0 {
			if bytes.Compare(rangeEndKey, subEndKey) <= 0 {
				rangeEndKey = subEndKey
			}
		} else {
			result = append(result, kv.KeyRange{StartKey: rangeStartKey, EndKey: rangeEndKey})
			rangeStartKey, rangeEndKey = subStartKey, subEndKey
		}
	}
	result = append(result, kv.KeyRange{StartKey: rangeStartKey, EndKey: rangeEndKey})
	return result
}

func (c *twoPhaseCommitter) vertifyReadSet(bo *Backoffer, action actionVertifyReadSet, ranges []kv.KeyRange) error {
	regionCache := c.store.GetRegionCache()
	locs, err := SplitKeyRangesByLocations(bo, ranges, regionCache)
	if err != nil {
		return err
	}
	return c.doVertifyReadSet(bo, action, locs)
}
func (c *twoPhaseCommitter) doVertifyReadSet(bo *retry.Backoffer, action actionVertifyReadSet, vertifyRanges []vertifyRange) error {
	if len(vertifyRanges) == 0 {
		return nil
	}
	rateLim := len(vertifyRanges)
	// Set rateLim here for the large transaction.
	// If the rate limit is too high, tikv will report service is busy.
	// If the rate limit is too low, we can't full utilize the tikv's throughput.
	// TODO: Find a self-adaptive way to control the rate limit here.
	if rateLim > config.GetGlobalConfig().CommitterConcurrency {
		rateLim = config.GetGlobalConfig().CommitterConcurrency
	}
	// batchExecutor := newBatchExecutor(rateLim, c, actionVertifyReadSet{}, bo)
	iBatch := make([]interface{}, 0)
	for i := 0; i < len(vertifyRanges); i++ {
		iBatch = append(iBatch, vertifyRanges[i])
	}
	// return batchExecutor.process(iBatch)
	return nil
}

type actionVertifyReadSet struct{ retry bool }

var _ twoPhaseCommitAction = actionVertifyReadSet{}

func (actionVertifyReadSet) String() string {
	return "vertify read set"
}

// 改造这个方法，使得这个方法可以在 BatchExecutor 中使用
func (action actionVertifyReadSet) handleSingleBatch(c *twoPhaseCommitter, bo *retry.Backoffer, locs interface{}) (err error) {
	scanRange, ok := locs.(vertifyRange)
	if !ok {
		return errors.New("unexcept type in actionVertifyReadSet")
	}
	tBegin := time.Now()
	attempts := 0
	req := c.buildVertifyReadSetRequest(scanRange)
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())
	for {
		attempts++
		if attempts > 1 || action.retry {
			req.IsRetryRequest = true
		}
		reqBegin := time.Now()
		if reqBegin.Sub(tBegin) > slowRequestThreshold {
			logutil.BgLogger().Warn("slow vertify request", zap.Uint64("startTS", c.startTS), zap.Stringer("region", &scanRange.loc.Region), zap.Int("attempts", attempts))
			tBegin = time.Now()
		}
		resp, err := sender.SendReq(bo, req, scanRange.loc.Region, client.ReadTimeoutMedium)
		if err != nil {
			return err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return err
		}
		if regionErr != nil {
			// For other region error and the fake region error, backoff because
			// there's something wrong.
			// For the real EpochNotMatch error, don't backoff.
			if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
				err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					return err
				}
			}
			err = c.vertifyReadSet(bo, actionVertifyReadSet{true}, []kv.KeyRange{scanRange.keyRange})
			return err
		}
		if resp.Resp == nil {
			return clientError.ErrBodyMissing
		}
		response := resp.Resp.(kvrpcpb.VertifyReadSetResponse)
		if response.SameReadSet {
			return nil
		}
		return clientError.ErrVertifyReadSet
	}
}

func (action actionVertifyReadSet) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return metrics.TxnRegionsNumHistogramPessimisticRollback
}

func (c *twoPhaseCommitter) buildVertifyReadSetRequest(scanRange vertifyRange) *tikvrpc.Request {
	request := &kvrpcpb.VertifyReadSetRequest{
		StartVersion:  c.startTS,
		CommitVersion: c.commitTS,
		StartKey:      scanRange.keyRange.StartKey,
		EndKey:        scanRange.keyRange.EndKey,
	}
	r := tikvrpc.NewRequest(tikvrpc.CmdVertifyReadSet, request, kvrpcpb.Context{
		Priority:         c.priority,
		SyncLog:          c.syncLog,
		ResourceGroupTag: c.resourceGroupTag,
	})
	return r
}
