package business

type UserManager struct {
	users map[uint64]*User
}

var manager = &UserManager{
	users: make(map[uint64]*User, 0),
}

func GetUser(userid uint64) (*User, bool) {
	var user, ok = manager.users[userid]

	return user, ok
}

func SetUser(user *User) {
	if client, ok := manager.users[user.Id]; ok {
		_ = client.Conn.Close()
	}
	manager.users[user.Id] = user
}

func DeleteUser(userid uint64) {
	delete(manager.users, userid)
}
