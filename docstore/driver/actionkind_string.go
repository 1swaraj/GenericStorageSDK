package driver

import "strconv"

func _() {

	var x [1]struct{}
	_ = x[Create-0]
	_ = x[Replace-1]
	_ = x[Put-2]
	_ = x[Get-3]
	_ = x[Delete-4]
	_ = x[Update-5]
}

const _ActionKind_name = "CreateReplacePutGetDeleteUpdate"

var _ActionKind_index = [...]uint8{0, 6, 13, 16, 19, 25, 31}

func (i ActionKind) String() string {
	if i < 0 || i >= ActionKind(len(_ActionKind_index)-1) {
		return "ActionKind(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _ActionKind_name[_ActionKind_index[i]:_ActionKind_index[i+1]]
}
