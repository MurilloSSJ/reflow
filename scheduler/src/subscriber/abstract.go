package subscriber

type AbstractSubscriber interface {
	Update(data []byte) error
}
