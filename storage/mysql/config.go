package mysql

type option func(*Mapper)

var Options singleton

type singleton struct{}

func (singleton) MessagesTableName(name string) option {
	return func(mapper *Mapper) { mapper.messagesTable = name }
}
func (singleton) SnapshotsTableName(name string) option {
	return func(mapper *Mapper) { mapper.snapshotsTable = name }
}
func (singleton) defaults() []option {
	return []option{
		Options.MessagesTableName("Messages"),
		Options.SnapshotsTableName("Snapshots"),
	}
}
