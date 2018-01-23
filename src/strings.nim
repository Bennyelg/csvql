import tables
export tables
import strutils 

proc `+`*(s: varargs[string, `$`]): string = 
  result = s[0]
  for i in 1..len(s)-1: result.add s[i]

template format*(statement: string, elements: varargs[string, `$`]): string =
  var i = 0
  var newS = statement
  for word in statement.split(" "):
    if word.contains("{") and word.contains("}"):
      newS = newS.replaceWord(word, $elements[i])
      i += 1
  newS

template format*[T](statement: string, elements: Table[string, T]): string =
  var newS = statement
  for word in statement.split(" "):
    if word.contains("{") and word.contains("}"):
      let term = word.replace("{", "").replace("}", "")
      newS = newS.replaceWord(word, $elements[term])
  newS

# when isMainModule:
#   echo("a" + "b" + "1" + 2 + 0.5)
#   echo("test {1}, string {2}, to be format {3} {4}".format("a", 1, "2", "c"))
#   echo("My name is: {name} and Im {age} years old".format({"name": "benny", "age": "100"}.toTable))