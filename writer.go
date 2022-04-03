package batch

import (
	"time"

	"github.com/sirupsen/logrus"
)

// 支持泛型
type BatchWriter [T any] struct {
	batchSize    int           // 批量大小
	batchTimeout time.Duration // 批量超时时间
	bufferCh     chan T
	writeBuf     []T
	writeCh      chan []T
	writeStop    chan struct{}
	bufferStop   chan struct{}
	bufferFlush  chan struct{}
	doneCh       chan struct{}
	writeFunc    func(datas []T)

	bufferInfoCh chan writeBuffInfoReq
	writeInfoCh  chan writeBuffInfoReq
}

type writeBuffInfoReq struct {
	writeBuffLen int
}

func NewBatchWriter[T any] (batchSize int, batchTimeout time.Duration, writeFunc func(datas []T)) *BatchWriter[T] {
	if batchTimeout < time.Second {
		batchTimeout = time.Second
	}

	writer := BatchWriter [T] {
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		bufferCh:     make(chan T, 1),
		writeCh:      make(chan []T, 1),
		writeBuf:     make([]T, 0, batchSize+1),
		writeStop:    make(chan struct{}),
		bufferStop:   make(chan struct{}),
		bufferFlush:  make(chan struct{}),
		doneCh:       make(chan struct{}),
		bufferInfoCh: make(chan writeBuffInfoReq),
		writeInfoCh:  make(chan writeBuffInfoReq),
		writeFunc:    writeFunc,
	}

	go writer.writeProc()
	go writer.bufferProc()
	return &writer
}

func (w *BatchWriter[T]) AppendData(data T) {
	w.bufferCh <- data
}

func (w *BatchWriter[T]) Close() {
	if w.writeCh != nil {
		// Flush outstanding metrics
		w.Flush()

		// stop and wait for buffer proc
		close(w.bufferStop)
		close(w.writeStop)
		<-w.doneCh

		close(w.writeCh)
		close(w.writeInfoCh)
		close(w.bufferInfoCh)
		w.writeCh = nil
		w.writeCh = nil
	}
	if w.bufferCh != nil {
		close(w.bufferCh)
		w.bufferCh = nil
	}
	logrus.Info("batch writer closed")
}

func (w *BatchWriter[T]) writeProc() {
	logrus.Trace("Write proc started")
x:
	for {
		select {
		case datas := <-w.writeCh:
			w.writeFunc(datas)
		case <-w.writeStop:
			logrus.Trace("Write proc: received stop")
			break x
		case buffInfo := <-w.writeInfoCh:
			buffInfo.writeBuffLen = len(w.writeCh)
			w.writeInfoCh <- buffInfo
		}
	}
}
func (w *BatchWriter[T]) bufferProc() {
	logrus.Trace("Buffer proc started")
	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()

x:
	for {
		select {
		case data := <-w.bufferCh:
			w.writeBuf = append(w.writeBuf, data)
			if len(w.writeBuf) >= w.batchSize {
				w.flushBuffer()
			}
		case <-ticker.C:
			w.flushBuffer()
		case <-w.bufferStop:
			w.flushBuffer()
			break x
		case buffInfo := <-w.bufferInfoCh:
			buffInfo.writeBuffLen = len(w.bufferInfoCh)
			w.bufferInfoCh <- buffInfo
		}
	}

	logrus.Trace("Buffer proc finished")
	w.doneCh <- struct{}{}
}

func (w *BatchWriter[T]) flushBuffer() {
	if len(w.writeBuf) > 0 {
		buf := make([]T, len(w.writeBuf))
		copy(buf, w.writeBuf)
		w.writeCh <- buf
		w.writeBuf = w.writeBuf[:0]
	}
}

// Flush forces all pending writes from the buffer to be sent
func (w *BatchWriter[T]) Flush() {
	w.bufferFlush <- struct{}{}
	w.waitForFlushing()
}

func (w *BatchWriter[T]) waitForFlushing() {
	for {
		w.bufferInfoCh <- writeBuffInfoReq{}
		writeBuffInfo := <-w.bufferInfoCh
		if writeBuffInfo.writeBuffLen == 0 {
			break
		}
		logrus.Info("Waiting buffer is flushed")
		<-time.After(time.Millisecond)
	}
	for {
		w.writeInfoCh <- writeBuffInfoReq{}
		writeBuffInfo := <-w.writeInfoCh
		if writeBuffInfo.writeBuffLen == 0 {
			break
		}
		logrus.Info("Waiting buffer is flushed")
		<-time.After(time.Millisecond)
	}
}

//func (w *BatchWriter) write(pointDatas []interface{}) {
//	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
//		Precision: "ms",
//		Database:  pointDatas[0].ProjectID,
//	})
//	if err != nil {
//		logrus.WithError(err).Warn("NewBatchPoints error")
//	} else {
//		for i := range pointDatas {
//			pointData := pointDatas[i]
//			tags := map[string]string{}
//			point, err := influxdb.NewPoint(pointData.DeviceID, tags, pointData.Fields, pointData.Timestamp)
//			if err == nil {
//				bp.AddPoint(point)
//			} else {
//				logrus.WithError(err).Warn("create point data error")
//			}
//		}
//		if err := w.client.Write(bp); err != nil {
//			logrus.WithError(err).Warn("influxDB Write")
//		}
//	}
//
//}
