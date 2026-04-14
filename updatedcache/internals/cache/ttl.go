package cache

import "time"

// StartJanitor runs periodic cleanup of expired entries
func (sc *ShardedCache) StartJanitor(interval time.Duration) {

	ticker := time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-ticker.C:
				for _, shard := range sc.shards {
					shard.cleanExpired()
				}
			case <-sc.stopJanitor:
				ticker.Stop()
				return
			}
		}
	}()
}
func (sc *ShardedCache) StopJanitor() {
	select {
	case <-sc.stopJanitor:
		// already closed
	default:
		close(sc.stopJanitor)
	}
}
