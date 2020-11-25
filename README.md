# ProperCR: Dealing with crowd annotations of semantic properties

This repository contains the code and dataset presented in the following paper (to appear at Coling 2020):

@inproceedings{sommerauer-etal-2020,
	Author = {Pia Sommerauer and Antske Fokkens and Piek Vossen},
	Booktitle = {Proceedings of the 28th International Conference on Computational Linguistics},
	Title = {Would you describe a leopard as yellow? Evaluating crowd-annotations with justified and informative disagreement},
	Year = {in press}}

Please cite this paper if you use our code or dataset. 

## Data

Aggregated labels (according to evaluation results):

- aggregated_labels/ 

The directory contains aggregated crowd labels with the best aggregation method on the level of fine-grained relations ('relations') and coarse-grained relations ('levels'). 

**Raw annotation output**

- `data/`

The raw output consists of the output downloaded from the annotation tool (set up with Lingoturk and placed in `prolific_output`) and summary data from prolific (worker information, duration of completing a task)

Versions:

* run1_group_experiment1: pilot run
* run3_group_experiment1: pilot run 
* run4_group_experiment2: first full run

All experiments were carried out with on the entire set. DEtailed statistics per run are listed in the paper. 

The remaining directories contain the raw output of the expert annotations. 

**Gold labels provided by experts**

- `gold_labels/`

The entire set of gold labels can be found in gold.csv. The gold labels have been aggregated following the procedure outlined in the paper. 

**File structure**

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

## Contradiction metric:

For our own metric based on worker-contradictions, we defined which relations count as contradictions. All contradictions are listed in:

-`scheme/contradictions.csv`


## Code

We evaluate various methods of aggregating annotations (based on various quality metrics) against gold annotations.

We ran the experiments on the full version of our dataset. It is also possibly to run it on portions of the dataset. By default, all scripts are processing the full dataset. The settings can be changed in the config.json. 

**1. Run annotation analyses**

Contradiction analysis (on the level of workers and pairs): 

`python analyze_pairs.py`

CrowdTruth analysis

`python run_crowdtruth.py`

**2. Evaluation against gold labels**

`python evaluation.py`

The results are written to `evaluation/`.

**3. Prediction of justified disagreement**

`python predict_disagreement.py`



## Contact:

Pia Sommerauer (pia.sommerauer@vu.nl)
Vrije Universiteit Amsterdam