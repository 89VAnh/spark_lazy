# import data

`spark-submit data/importStackoverFlow.py`

# fake data

`spark-submit data/fakeData.py`

# export data

`spark-submit data/exportStackoverFlow.py`

# package scala file

`scala-cli --power package scala_file --assembly --preamble=false -f`

# run jar file

`spark-submit jar_file.jar`
