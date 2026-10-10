package q

import (
	"strings"
	"testing"
)

func TestMatcherShapes(t *testing.T) {
	c, ok := Eq("Site", "OTT").(Comparison)
	if !ok || c.Field != "Site" || c.Op != OpEq || c.Value != "OTT" {
		t.Fatalf("Eq = %#v", Eq("Site", "OTT"))
	}
	if Ne("A", 1).(Comparison).Op != OpNe {
		t.Fatal("Ne op")
	}
	if Gt("A", 1).(Comparison).Op != OpGt {
		t.Fatal("Gt op")
	}
	if Gte("A", 1).(Comparison).Op != OpGte {
		t.Fatal("Gte op")
	}
	if Lt("A", 1).(Comparison).Op != OpLt {
		t.Fatal("Lt op")
	}
	if Lte("A", 1).(Comparison).Op != OpLte {
		t.Fatal("Lte op")
	}
	in, ok := In("Site", "A", "B").(InMatcher)
	if !ok || in.Field != "Site" || len(in.Values) != 2 {
		t.Fatalf("In = %#v", in)
	}
	and, ok := And(Eq("A", 1), Eq("B", 2)).(AndMatcher)
	if !ok || len(and.Matchers) != 2 {
		t.Fatalf("And = %#v", and)
	}
	or, ok := Or(Eq("A", 1)).(OrMatcher)
	if !ok || len(or.Matchers) != 1 {
		t.Fatalf("Or = %#v", or)
	}
	not, ok := Not(Eq("A", 1)).(NotMatcher)
	if !ok || not.Matcher == nil {
		t.Fatalf("Not = %#v", not)
	}
}

func TestParam(t *testing.T) {
	a, b := Param(), Param()
	if a == b {
		t.Fatal("Param values are not distinct")
	}
	if a.String() != "?" {
		t.Fatalf("Param.String = %q, want ?", a.String())
	}
	if desc := Describe(Eq("Site", a)); !strings.Contains(desc, "?") {
		t.Fatalf("Describe param = %q, want a placeholder", desc)
	}
}

func TestAggregateShapes(t *testing.T) {
	if Count().Op != OpCount || Count().Field != "" {
		t.Fatalf("Count = %#v", Count())
	}
	sum := Sum("Score")
	if sum.Op != OpSum || sum.Field != "Score" || sum.Op.String() != "SUM" {
		t.Fatalf("Sum = %#v", sum)
	}
	if Avg("Score").Op != OpAvg || Min("Name").Op != OpMin || Max("Rank").Op != OpMax {
		t.Fatal("aggregate ops")
	}
	if AggOp(99).String() == "" {
		t.Fatal("AggOp String is empty")
	}
}

func TestDescribe(t *testing.T) {
	desc := Describe(And(Eq("Site", "OTT"), Or(Gte("Status", 2), Not(In("Site", "X")))))
	for _, want := range []string{"Site", "OTT", "Status", "AND", "OR", "NOT", "IN"} {
		if !strings.Contains(desc, want) {
			t.Fatalf("Describe = %q, missing %q", desc, want)
		}
	}
	if Describe(nil) == "" {
		t.Fatal("Describe(nil) is empty")
	}
}

func TestConvenienceMatcherShapes(t *testing.T) {
	between := Between("Rank", 2, 5).(AndMatcher)
	if len(between.Matchers) != 2 || between.Matchers[0].(Comparison).Op != OpGte || between.Matchers[1].(Comparison).Op != OpLte {
		t.Fatal(between)
	}
	if _, ok := NotIn("Site", "A").(NotMatcher).Matcher.(InMatcher); !ok {
		t.Fatal("NotIn shape")
	}
	for _, matcher := range []Matcher{StartsWith("Name", "a"), EndsWith("Name", "a"), Contains("Name", "a"), Like("Name", "a%")} {
		if _, ok := matcher.(StringMatcher); !ok {
			t.Fatal(matcher)
		}
		if !strings.Contains(Describe(matcher), "Name") || !strings.Contains(Describe(matcher), "a") {
			t.Fatal(Describe(matcher))
		}
	}
}
