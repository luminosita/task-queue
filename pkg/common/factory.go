package common

type Connection interface{}

type Factory struct{}

func (Factory) NewConnection() (conn *Connection, err error) {
	return nil, nil
}
