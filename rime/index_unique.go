package rime

// uniqueIndex documents unique-index behavior; storage lives in indexSet
// (index.go) as field -> value -> key maps. Primary keys are always unique.
// Uniqueness is enforced at commit time inside the commit lock, against live
// state plus tentative same-transaction claims, so concurrent committers
// cannot both claim one value.
