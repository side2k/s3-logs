QUOTE_LEFT = '["'
QUOTE_RIGHT = ']"'

def parse_log(lines):
    parsed = []
    for line in lines:
        if line:
            fields = []
            tail = line.strip()
            while tail:
                field, _, tail = tail.partition(' ')
                #print 'field is: "%s"(%s)' % (field, tail)
                if field[0] in QUOTE_LEFT:
                    delimiter_index = QUOTE_LEFT.index(field[0])
                    quote_right = QUOTE_RIGHT[delimiter_index]
                    field_right, _, tail = tail.partition(quote_right)
                    tail = tail.strip()
                    fields += ['%s %s' % (field[1:], field_right)]
                else:
                    fields += [field]
            parsed += [fields]
    return parsed
        
f = open('logs/2011-10-14-00-16-56-B066D716F32D84BA', 'r')
lines = f.readlines()
f.close()
parsed_data = parse_log(lines)        
for field in parsed_data[0]:
    print field