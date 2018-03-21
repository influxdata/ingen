package ingen

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	DB        DBConfig
	Generator GeneratorConfig
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) Validate() error {
	// build default values
	def := &configDefaults{}
	WalkConfig(def, c)

	// validate
	val := &configValidator{}
	WalkConfig(val, c)
	return val.Err()
}

type GeneratorConfig struct {
	TagPrefix string `toml:"tag-prefix"`
	Tags      []Tag
}

type Tag struct {
	Name     string
	Sequence string
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

type Visitor interface {
	Visit(node Node) Visitor
}

type Node interface{ node() }

func (*Config) node()          {}
func (*DBConfig) node()        {}
func (*GeneratorConfig) node() {}

func WalkConfig(v Visitor, node Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *Config:
		WalkConfig(v, &n.DB)
		WalkConfig(v, &n.Generator)

	case *DBConfig:

	case *GeneratorConfig:

	default:
		panic(fmt.Sprintf("ast.Walk: unexpected node type %T", n))
	}
}

type configValidator struct {
	errs []error
}

func (v *configValidator) Visit(node Node) Visitor {
	switch n := node.(type) {
	case *DBConfig:
		if n.StartTime.Add(n.TimeSpan()).After(time.Now()) {
			v.errs = append(v.errs, fmt.Errorf("start time must be ≤ %s", time.Now().Truncate(n.ShardDuration.Duration).UTC().Add(-n.TimeSpan())))
		}

	case *GeneratorConfig:
		break
		if len(n.Tags) == 0 {
			v.errs = append(v.errs, errors.New("no tags defined"))
			return nil
		}

		names := make(map[string]struct{})
		for i := range n.Tags {
			t := &n.Tags[i]
			if _, ok := names[t.Name]; ok {
				v.errs = append(v.errs, fmt.Errorf("duplicate tag name: %s", t.Name))
			} else {
				names[t.Name] = struct{}{}
			}
		}
	}

	return v
}
func (v *configValidator) Err() error {
	return errorList(v.errs)
}

type configDefaults struct {
	tagN int
}

func (v *configDefaults) Visit(node Node) Visitor {
	switch n := node.(type) {
	case *DBConfig:
		if n.DataPath == "" {
			n.DataPath = "${HOME}/.influxdb/data"
		}
		if n.MetaPath == "" {
			n.MetaPath = "${HOME}/.influxdb/meta"
		}
		if n.Database == "" {
			n.Database = "db"
		}
		if n.RP == "" {
			n.RP = "autogen"
		}
		if n.ShardDuration.Duration == 0 {
			n.ShardDuration.Duration = 24 * time.Hour
		}
		if n.ShardCount == 0 {
			n.ShardCount = 1
		}
		if n.StartTime.IsZero() {
			n.StartTime = time.Now().Truncate(n.ShardDuration.Duration).Add(-n.TimeSpan())
		}

	case *GeneratorConfig:
		if len(n.TagPrefix) == 0 {
			n.TagPrefix = "tag"
		}

		for i := range n.Tags {
			t := &n.Tags[i]

			// assign default names
			if len(t.Name) == 0 {
				t.Name = fmt.Sprintf("%s%d", n.TagPrefix, v.tagN)
				v.tagN++
			}
		}
	}

	return v
}

// ErrorList is a simple error aggregator to return multiple errors as one.
type ErrorList []error

func (el ErrorList) Error() string {
	b := new(strings.Builder)
	for _, err := range el {
		b.WriteString(err.Error())
		b.WriteByte('\n')
	}
	return b.String()
}

func errorList(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return ErrorList(errs)
}
