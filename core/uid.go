package core

import (
	"fmt"
	"sync"
	"time"
)

var count int64
var oldUseTimeStamp = time.Now().Unix()
var mux sync.Mutex

const PerSecondMaxValue int64 = 10000

func init() {
	count = 0
}

func GenID() string {
	defer mux.Unlock()
	mux.Lock()
	if count > PerSecondMaxValue {
		oldUseTimeStamp = time.Now().Unix()
		count = 0
	}
	count++
	return fmt.Sprintf("%v", oldUseTimeStamp*PerSecondMaxValue+count)
}
