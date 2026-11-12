package specgen

// LookupField returns one common field from the JSONata or JSONPath table.
func LookupField(language, name string) (Field, bool) {
	for _, lang := range Languages {
		if lang.Name != language {
			continue
		}
		for _, field := range lang.Fields {
			if field.Name == name {
				return field, true
			}
		}
	}
	return Field{}, false
}
