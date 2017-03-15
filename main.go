package main

import "github.com/garyburd/redigo/redis"
import "time"
import "flag"
import "sync"
import "fmt"
import "runtime"
import "encoding/json"
// import "gopkg.in/redsync.v1"
import "github.com/satori/go.uuid"

type MyStruct struct {
  Values []int
}

func newPool(addr string) *redis.Pool {
  return &redis.Pool{
    MaxIdle: 3,
    IdleTimeout: 240 * time.Second,
    Dial: func () (redis.Conn, error) { return redis.Dial("tcp", addr, redis.DialDatabase(1)) },
  }
}

var (
  pool *redis.Pool
  redisServer = flag.String("redisServer", "localhost:6379", "")
)

var concurrency = 100

func main() {
  runtime.GOMAXPROCS(4)

  fmt.Println("redis go starts")
  flag.Parse()
  pool = newPool(*redisServer)
  conn := pool.Get()
  defer conn.Close()
  // r := redsync.New([]redsync.Pool{pool})
  _, err := conn.Do("DEL", "testing")
  if err != nil {
    fmt.Println("Failed cleanup testing beforehand")
    return
  }

  start := time.Now()
  var wg sync.WaitGroup
  for i := 1; i <= concurrency; i++ {
    wg.Add(1)
    go func(count int){
      defer wg.Done()
      // m := r.NewMutex("testing_lock")
      // m.Lock()
      // defer m.Unlock()
      conn := pool.Get()
      defer conn.Close()

      lockIdentifier := acquireLockWithTimeout(pool, "testing", 10000, 1000)
      if lockIdentifier == "" {
        fmt.Println("Failed to acquire a lock")
        return
      }
      defer func(lockID string) {
        releaseLock(pool, "testing", lockID)
      }(lockIdentifier)

      val, err := redis.String(conn.Do("GET", "testing"))
      if err != nil || val == "" {
        myStructVal := MyStruct{
          Values: []int{count},     
        }
        testing, err := json.Marshal(myStructVal)
        if err != nil {
          fmt.Println("json marshal error:", err)
          return
        }
        _, err = conn.Do("SET", "testing", string(testing))
        if err != nil {
          fmt.Println("Failed to write: ", err)
        }
        return
      }

      unmarshaledVal := &MyStruct{}
      err = json.Unmarshal([]byte(val), unmarshaledVal)
      if err != nil {
        fmt.Println("json unmarshal error:", err)
        return
      }
      unmarshaledVal.Values = append(unmarshaledVal.Values, count)
      saveVal, err := json.Marshal(unmarshaledVal)
      if err != nil {
        fmt.Println("json marshal error:", err)
        return
      }
      _, err = conn.Do("SET", "testing", saveVal)
      if err != nil {
        fmt.Println("Failed to write: ", err)
      }
    }(i)
  }
  fmt.Println("active connetions: ", pool.ActiveCount())
  wg.Wait()
  fmt.Println("it took ", time.Since(start))

  val, err := redis.String(conn.Do("GET", "testing"))
  unmarshaledVal := &MyStruct{}
  err = json.Unmarshal([]byte(val), unmarshaledVal)
  if err != nil {
    fmt.Println("json unmarshal error:", err)
    return
  }
  missing := []int{}
  for i:=1; i<concurrency; i++ {
    if !intContains(unmarshaledVal.Values, i) {
      missing = append(missing, i)
    }
  }
  if len(missing) > 0 {
    fmt.Println("missing ", missing)
  } else {
    fmt.Println("All good, no missing values")
  }
}

func intContains(list []int, value int) bool {
  for _, val := range list {
    if val == value {
      return true
    }
  }
  return false
}

func acquireLockWithTimeout(
  pool *redis.Pool,
  lockName string,
  acquireTimeout int,
  lockTimeout int,
) string {
  identifier := uuid.NewV4().String()
  lockKey := fmt.Sprintf("lock:%v", lockName)
  lockExpire := lockTimeout / 1000

  end := time.Now().Unix() + int64(acquireTimeout / 1000)
  conn := pool.Get()
  defer conn.Close()
  for time.Now().Unix() < end {
    setnx, err := redis.Int(conn.Do("SETNX", lockKey, identifier))
    if err == nil && setnx == 1 {
      conn.Do("EXPIRE", lockKey, lockExpire)
      return identifier
    }
    if err != nil {
      fmt.Println("SETNX error", err)
    }
    ttl, err := redis.Int(conn.Do("TTL", lockKey))
    if err != nil {
      fmt.Println("TTL error", err)
    }
    if err != nil || ttl < 0 {
      conn.Do("EXPIRE", lockKey, lockExpire)
    }
    time.Sleep(1 * time.Millisecond)
  }
  return ""
}

func releaseLock(pool *redis.Pool, lockName string, identifier string) bool {
  conn := pool.Get()
  defer conn.Close()
  lockKey := fmt.Sprintf("lock:%v", lockName)
  for {
    conn.Do("WATCH", lockKey)
    currentLocker, err := redis.String(conn.Do("GET", lockKey))

    if err == nil && identifier == currentLocker {
      conn.Send("MULTI")
      conn.Send("DEL", lockKey)
      r, err := conn.Do("EXEC")
      if r == nil || err != nil {
        continue
      }
      return true
    }
    conn.Do("UNWATCH")
    break
  }
  return false
}