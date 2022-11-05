package rabbitmq

const (
	exchangeNameRealtime = "x.realtime"
	exchangeNameTopic    = "x.realtime.scope[%s].topic[%s]"
	queueNameSubscriber  = "q.realtime.scope[%s].topic[%s].subscriber[%s]"
)
