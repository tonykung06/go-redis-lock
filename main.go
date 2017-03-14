package main

import "github.com/garyburd/redigo/redis"
import "time"
import "flag"
import "sync"
import "fmt"
import "runtime"
import "encoding/json"
import "gopkg.in/redsync.v1"

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

func main() {
  runtime.GOMAXPROCS(4)

  fmt.Println("redis go starts")
  flag.Parse()
  pool = newPool(*redisServer)
  conn := pool.Get()
  defer conn.Close()
  r := redsync.New([]redsync.Pool{pool})
  _, err := conn.Do("DEL", "testing")
  if err != nil {
    fmt.Println("Failed cleanup testing beforehand")
    return
  }

  var wg sync.WaitGroup
  for i := 1; i <= 1000; i++ {
    wg.Add(1)
    go func(count int){
      defer wg.Done()
      m := r.NewMutex("testing_lock")
      m.Lock()
      defer m.Unlock()
      conn := pool.Get()
      defer conn.Close()

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
  wg.Wait()

  val, err := redis.String(conn.Do("GET", "testing"))
  unmarshaledVal := &MyStruct{}
  err = json.Unmarshal([]byte(val), unmarshaledVal)
  if err != nil {
    fmt.Println("json unmarshal error:", err)
    return
  }
  missing := []int{}
  for i:=1; i<1000; i++ {
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