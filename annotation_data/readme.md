# ProperCR

This directory contains a dataset of properties and concepts annotated with fine-grained semantic relations. Among other things, the dataset can be used for diagnostic experiments on latent language model representations (e.g. word embeddings).

This directory is structured as follows:

* crowd_annotations:

  Contains the raw crowd annotations sorted in subdirectories representing 3 versions of the dataset. Version 1 and 2 were pilots.
  Each version directory contains batch files. A single batch contains all relation-concept-property triples given to 10 crowd annotators at a time.

* gold_annotations:

  Contains triples annotated by three experts (the authors of the paper). The triples can be matched with the crowd annotations. The file shows the aggregated expert judgments after discussion.

* contradictions.csv: CSV file listing all relation-pairs counted as contradictions. Pair annotations of a worker who selected contradictory relations may be of lower quality (discussed in the paper).


The annotation files (gold and crowd) are in csv format and are structured as follows:


Columns relevant for the task:

* triple: relation-property-concept triple
* description: Natural language description of relation between property and concept
* exampletrue: An example of a description most people would probably judge as true (i.e. select 'agree' in the task)
* examplefalse: An example of a description most people would probably judge as false (i.e. select 'disagree' in the task)
* answer: answer given by a worker
* workerid: Prolific id of a worker
* timestamp: Time submitted
* name: task name
* quid: question id
* run: question version (we had multiple versions for the phrasing)


Columns with information about the task in Prolific and Lingoturk:
* completionurl
* questionid
* filename
* listnumber
* assignmentid
* hitid
* origin
* partid


Each row contains the response of a worker.

The data will be made freely available under a Creative Commons license.
