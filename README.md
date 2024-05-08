# wayfare assignment


Creates a lineage graph from a sources.yml file through various sql models, including the datatypes of the columns. 

how to run:

`cargo run -- <sources.yml> <dir_with_sql_models/>`

you can generate the image with the `graphviz` commandline tool. for example:

`dot -Tpng graph.dot > graph.png`. 

note: does not support extracting datatype from json object atm.
