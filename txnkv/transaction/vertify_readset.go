package transaction

import (
	"bytes"
	"errors"
	"sort"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	zap "go.uber.org/zap"
)

// SplitKeyRangesByLocations splits the KeyRanges by logical info in the cache.
func SplitKeyRangesByLocations(bo *retry.Backoffer, ranges *kv.KeyRanges, regionCache *locate.RegionCache) ([]*LocationKeyRanges, error) {

	res := make([]*LocationKeyRanges, 0)
	for ranges.Len() > 0 {
		loc, err := regionCache.LocateKey(bo, ranges.At(0).StartKey)
		if err != nil {
			return res, err
		}

		// Iterate to the first range that is not complete in the region.
		var r kv.KeyRange
		var i int
		for ; i < ranges.Len(); i++ {
			r = ranges.At(i)
			if !(loc.Contains(r.EndKey) || bytes.Equal(loc.EndKey, r.EndKey)) {
				break
			}
		}
		// All rest ranges belong to the same region.
		if i == ranges.Len() {
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: ranges})
			break
		}

		if loc.Contains(r.StartKey) {
			// Part of r is not in the region. We need to split it.
			taskRanges := ranges.Slice(0, i)
			taskRanges.Last = &kv.KeyRange{
				StartKey: r.StartKey,
				EndKey:   loc.EndKey,
			}
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: taskRanges})

			ranges = ranges.Slice(i+1, ranges.Len())
			ranges.First = &kv.KeyRange{
				StartKey: loc.EndKey,
				EndKey:   r.EndKey,
			}
		} else {
			// rs[i] is not in the region.
			taskRanges := ranges.Slice(0, i)
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: taskRanges})
			ranges = ranges.Slice(i, ranges.Len())
		}
	}
	return res, nil
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
			result = append(result, kv.KeyRange{rangeStartKey, rangeEndKey})
			rangeStartKey, rangeEndKey = subStartKey, subEndKey
		}
	}
	return result
}

// SplitKeyRangesByBuckets splits the KeyRanges by buckets information in the cache. If regions don't have buckets,
// it's equal to SplitKeyRangesByLocations.
//
// TODO(youjiali1995): Try to do it in one round and reduce allocations if bucket is not enabled.
func SplitKeyRangesByBuckets(bo *retry.Backoffer, ranges *kv.KeyRanges, regionCache *locate.RegionCache) ([]*LocationKeyRanges, error) {
	locs, err := SplitKeyRangesByLocations(bo, ranges, regionCache)
	if err != nil {
		return nil, err
	}
	res := make([]*LocationKeyRanges, 0, len(locs))
	for _, loc := range locs {
		res = append(res, loc.SplitKeyRangesByBuckets()...)
	}
	return res, nil
}

// LocationKeyRanges wrapps a real Location in PD and its logical ranges info.
type LocationKeyRanges struct {
	// Location is the real location in PD.
	Location *locate.KeyLocation
	// Ranges is the logic ranges the current Location contains.
	Ranges *kv.KeyRanges
}

func (l *LocationKeyRanges) getBucketVersion() uint64 {
	return l.Location.GetBucketVersion()
}

// splitKeyRangeByBuckets splits ranges in the same location by buckets and returns a LocationKeyRanges array.
func (l *LocationKeyRanges) SplitKeyRangesByBuckets() []*LocationKeyRanges {
	if l.Location.Buckets == nil || len(l.Location.Buckets.Keys) == 0 {
		return []*LocationKeyRanges{l}
	}

	ranges := l.Ranges
	loc := l.Location
	res := []*LocationKeyRanges{}
	for ranges.Len() > 0 {
		// ranges must be in loc.region, so the bucket returned by loc.LocateBucket is guaranteed to be not nil
		bucket := loc.LocateBucket(ranges.At(0).StartKey)

		// Iterate to the first range that is not complete in the bucket.
		var r kv.KeyRange
		var i int
		for ; i < ranges.Len(); i++ {
			r = ranges.At(i)
			if !(bucket.Contains(r.EndKey) || bytes.Equal(bucket.EndKey, r.EndKey)) {
				break
			}
		}
		// All rest ranges belong to the same bucket.
		if i == ranges.Len() {
			res = append(res, &LocationKeyRanges{l.Location, ranges})
			break
		}

		if bucket.Contains(r.StartKey) {
			// Part of r is not in the bucket. We need to split it.
			taskRanges := ranges.Slice(0, i)
			taskRanges.Last = &kv.KeyRange{
				StartKey: r.StartKey,
				EndKey:   bucket.EndKey,
			}
			res = append(res, &LocationKeyRanges{l.Location, taskRanges})

			ranges = ranges.Slice(i+1, ranges.Len())
			ranges.First = &kv.KeyRange{
				StartKey: bucket.EndKey,
				EndKey:   r.EndKey,
			}
		} else {
			// ranges[i] is not in the bucket.
			taskRanges := ranges.Slice(0, i)
			res = append(res, &LocationKeyRanges{l.Location, taskRanges})
			ranges = ranges.Slice(i, ranges.Len())
		}
	}
	return res
}

func (c *twoPhaseCommitter) vertifyReadSet(bo *retry.Backoffer, action actionVertifyReadSet, ranges *kv.KeyRanges) error {
	regionCache := c.store.GetRegionCache()
	vertifyBo := retry.NewBackofferWithVars(c.store.Ctx(), CommitSecondaryMaxBackoff, c.txn.vars)
	locs, err := SplitKeyRangesByBuckets(vertifyBo, ranges, regionCache)
	if err != nil {
		return err
	}
	return c.doVertifyReadSet(vertifyBo, action, locs)
}
func (c *twoPhaseCommitter) doVertifyReadSet(bo *retry.Backoffer, action actionVertifyReadSet, locs []*LocationKeyRanges) error {
	if len(locs) == 0 {
		return nil
	}
	rateLim := len(locs)
	// Set rateLim here for the large transaction.
	// If the rate limit is too high, tikv will report service is busy.
	// If the rate limit is too low, we can't full utilize the tikv's throughput.
	// TODO: Find a self-adaptive way to control the rate limit here.
	if rateLim > config.GetGlobalConfig().CommitterConcurrency {
		rateLim = config.GetGlobalConfig().CommitterConcurrency
	}
	batchExecutor := newBatchExecutor(rateLim, c, actionVertifyReadSet{}, bo)
	iBatch := make([]interface{}, 0)
	for i := 0; i < len(locs); i++ {
		iBatch = append(iBatch, locs[i])
	}
	return batchExecutor.process(iBatch)
}

type actionVertifyReadSet struct{ retry bool }

var _ twoPhaseCommitAction = actionVertifyReadSet{}

func (actionVertifyReadSet) String() string {
	return "vertify read set"
}

// 改造这个方法，使得这个方法可以在 BatchExecutor 中使用
func (action actionVertifyReadSet) handleSingleBatch(c *twoPhaseCommitter, bo *retry.Backoffer, locs interface{}) (err error) {
	loc, ok := locs.(*LocationKeyRanges)
	if !ok {
		return errors.New("unexcept type in actionVertifyReadSet")
	}
	tBegin := time.Now()
	attempts := 0
	req := c.buildVertifyReadSetRequest(c.txn.startTS, c.txn.commitTS, loc)
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())
	for {
		attempts++
		if attempts > 1 || action.retry {
			req.IsRetryRequest = true
		}
		reqBegin := time.Now()
		if reqBegin.Sub(tBegin) > slowRequestThreshold {
			logutil.BgLogger().Warn("slow vertify request", zap.Uint64("startTS", c.startTS), zap.Stringer("region", &loc.Location.Region), zap.Int("attempts", attempts))
			tBegin = time.Now()
		}
		resp, err := sender.SendReq(bo, req, loc.Location.Region, client.ReadTimeoutShort)
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
			err = c.vertifyReadSet(bo, actionVertifyReadSet{true}, loc.Ranges)
			return err
		}
		if resp.Resp == nil {
			return tikverr.ErrBodyMissing
		}
		response := resp.Resp.(kvrpcpb.VertifyReadSetResponse)
		if response.SameReadSet {
			return nil
		}
		return tikverr.ErrVertifyReadSet
	}
}

func (action actionVertifyReadSet) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return metrics.TxnRegionsNumHistogramPessimisticRollback
}

func (c *twoPhaseCommitter) buildVertifyReadSetRequest(startTs, commitTs uint64, loc *LocationKeyRanges) *tikvrpc.Request {
	ranges := make([]*kvrpcpb.KeyRange, loc.Ranges.Len())
	for i := 0; i < loc.Ranges.Len(); i++ {
		ranges[i].StartKey = loc.Ranges.At(i).StartKey
		ranges[i].EndKey = loc.Ranges.At(i).EndKey
	}
	request := &kvrpcpb.VertifyReadSetRequest{
		StartVersion:  startTs,
		CommitVersion: commitTs,
		Range:         ranges,
	}
	r := tikvrpc.NewRequest(tikvrpc.CmdVertifyReadSet, request, kvrpcpb.Context{
		Priority:               c.priority,
		SyncLog:                c.syncLog,
		ResourceGroupTag:       c.resourceGroupTag,
		DiskFullOpt:            c.diskFullOpt,
		MaxExecutionDurationMs: uint64(client.MaxWriteExecutionTime.Milliseconds()),
		RequestSource:          c.txn.GetRequestSource(),
	})
	return r
}
