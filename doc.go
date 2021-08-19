package bow

//go:generate genius tmpl -d datatypes.yml -p ./ bowbuffer.gen.go.tmpl
//go:generate genius tmpl -d datatypes.yml -p ./ bowseries.gen.go.tmpl
//go:generate genius tmpl -d datatypes.yml -p ./ bowappend.gen.go.tmpl
//go:generate genius tmpl -d datatypes.yml -p ./ bowjoin.gen.go.tmpl
