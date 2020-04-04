package consumer

type EventHandler interface {
	OnStartConsume()
	OnEndConsume()

	OnReserveTimeout()
	OnQuitSignalTimeout()
}
