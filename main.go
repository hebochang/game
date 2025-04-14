package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

const (
	leaderboardKey = "leaderboard"
	maxTime        = 4900147200
)

type RankInfo struct {
	PlayerID string
	Score    int64
	Rank     int64
}

// 一、基础功能实现
func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	svc := NewLeaderboardService(rdb)
	now := time.Now().Unix()
	pip := rdb.Pipeline()
	svc.UpdateScore(pip, "玩家A", 100, now-10)
	svc.UpdateScore(pip, "玩家B", 100, now-11)
	svc.UpdateScore(pip, "玩家C", 95, now)
	svc.UpdateScore(pip, "玩家D", 95, now-2)
	svc.UpdateScore(pip, "玩家E", 101, now-2)

	_, err := pip.Exec(svc.ctx)
	if err != nil {
		logrus.Println(err)
		return
	}

	top, _ := svc.GetTopN(10)
	for _, t := range top {
		logrus.Println("GetTopN", t)
	}

	rank, _ := svc.GetPlayerRank("player1")
	logrus.Printf("player1 rank: %+v\n", rank)

	around, _ := svc.GetPlayerRankRange("玩家A", 1)
	logrus.Println("Around player2:")
	for _, a := range around {
		logrus.Println(a)
	}
}

type LeaderboardService interface {
	UpdateScore(pip redis.Pipeliner, playerId string, score int64, timestamp int64) error
	GetPlayerRank(playerId string) (*RankInfo, error)
	GetTopN(n int64) ([]*RankInfo, error)
	GetPlayerRankRange(playerId string, rangeNum int64) ([]*RankInfo, error)
}

type RedisLeaderboardService struct {
	rdb *redis.Client
	ctx context.Context
}

func NewLeaderboardService(rdb *redis.Client) *RedisLeaderboardService {
	return &RedisLeaderboardService{
		rdb: rdb,
		ctx: context.Background(),
	}
}

// 计算用于排序的 Redis ZSet 分数
func calcZSetScore(score int64, timestamp int64) float64 {
	float, _ := strconv.ParseFloat(fmt.Sprintf("%d.%d", score, maxTime-int(timestamp)), 64)
	return float
}

func parseZSetScore(zScore float64) (score int64, timestamp int64) {
	zScoreStr := strconv.FormatFloat(zScore, 'f', -1, 64)
	parts := strings.SplitN(zScoreStr, ".", 2)
	s, _ := strconv.Atoi(parts[0])
	score = int64(s)
	t, _ := strconv.Atoi(parts[1])
	timestamp = int64(maxTime - t)
	return
}

// 更新玩家分数
func (l *RedisLeaderboardService) UpdateScore(pip redis.Pipeliner, playerId string, score int64, timestamp int64) error {
	zScore := calcZSetScore(score, timestamp)
	return l.rdb.ZAdd(l.ctx, leaderboardKey, redis.Z{
		Score:  zScore,
		Member: playerId,
	}).Err()
}

// 获取玩家当前排名
func (l *RedisLeaderboardService) GetPlayerRank(playerId string) (*RankInfo, error) {
	rank, err := l.rdb.ZRevRank(l.ctx, leaderboardKey, playerId).Result()
	if err != nil {
		return nil, err
	}

	zScore, err := l.rdb.ZScore(l.ctx, leaderboardKey, playerId).Result()
	if err != nil {
		return nil, err
	}

	score, _ := parseZSetScore(zScore)
	return &RankInfo{
		PlayerID: playerId,
		Score:    score,
		Rank:     rank + 1,
	}, nil
}

// 获取排行榜前N名
func (l *RedisLeaderboardService) GetTopN(n int64) ([]*RankInfo, error) {
	results, err := l.rdb.ZRevRangeWithScores(l.ctx, leaderboardKey, 0, n-1).Result()
	if err != nil {
		return nil, err
	}

	var rankInfos []*RankInfo
	for i, z := range results {
		score, _ := parseZSetScore(z.Score)
		rankInfos = append(rankInfos, &RankInfo{
			PlayerID: z.Member.(string),
			Score:    score,
			Rank:     int64(i + 1),
		})
	}

	return rankInfos, nil
}

// 获取玩家周边排名
func (l *RedisLeaderboardService) GetPlayerRankRange(playerId string, rangeNum int64) ([]*RankInfo, error) {
	rank, err := l.rdb.ZRevRank(l.ctx, leaderboardKey, playerId).Result()
	if err != nil {
		return nil, err
	}

	start := int64(0)
	if rank-rangeNum > 0 {
		start = rank - rangeNum
	}
	end := rank + rangeNum

	results, err := l.rdb.ZRevRangeWithScores(l.ctx, leaderboardKey, start, end).Result()
	if err != nil {
		return nil, err
	}

	var rankInfos []*RankInfo
	for i, z := range results {
		score, _ := parseZSetScore(z.Score)
		rankInfos = append(rankInfos, &RankInfo{
			PlayerID: z.Member.(string),
			Score:    score,
			Rank:     start + int64(i) + 1,
		})
	}

	return rankInfos, nil
}

// 二、系统设计
/*可靠性要求
使用mysql做数据落盘写入存储，使用redis做缓存处理，并且开启AOF和RDB机制保证数据持久化
*/

/*性能要求
百万级使用单一ZSet只存储uid查询可能会有性能问题,查询时间复杂度是O(log N)级别
前5000名可以用redis进行存储，之后排行榜的数据可以使用 索引+mysql 存储
方案如下
1 API 服务
2 写入消息队列Kafka
3 异步消费者写入 MySQL
4 每1秒刷新排行榜缓存（TopN）到 Redis 或内存
*/

// 三、游戏需求更改（选做）
/*
思路：使用mysql Rank(), 可以自动处理并列排名，并跳过名次
定时任务，读取MySQL，按分数排序，写入Redis ZSET
*/
