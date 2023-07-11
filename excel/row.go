package excel

type Test struct {
	Name      string         `json:"name"`
	Descr     string         `json:"description"`
	Class     string         `json:"class"`
	Type      []string       `json:"type"`
	Address   map[string]any `json:"address"`
	Students  int            `json:"students"`
	Location  string         `json:"location"`
	StatusLog []string       `json:"status_log"`
}
