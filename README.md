## PheKnowVec
PheKnowVec is a novel method for deriving, implementing, and validating computational phenotypes. PheKnowVec leverages standardized clinical terminologies and open biomedical ontologies to derive, implement, and validate computational phenotype definitions in a scalable embedded structure.

Please see the [Project Wiki](https://github.com/callahantiff/PheKnowVec/wiki) for more information!


<br>

#### This is a Reproducible Research Repository
This repository contains more than just code, it provides a detailed and transparent narrative of our research process. For detailed information on how we use GitHub as a reproducible research platform, click [here](https://github.com/callahantiff/PheKnowVec/wiki/Using-GitHub-as-a-Reproducible-Research-Platform).

<img src="https://img.shields.io/badge/ReproducibleResearch-AbraCollaboratory-magenta.svg?style=flat-square" alt="git-AbraCollaboratory">

<br>

**Preliminary results were presented at the 2020 Joint Meeting of the American Medical Informatics Association:**  
Callahan TJ, Wyrwa J, Trinkley KE, Hunter LE, Kahn MG, Bennett TD. (2020, March). Towards Automating Computational Phenotyping: Exploring the Trade-offs of Different Vocabulary Mapping Strategies. Talk; Informatics Summits of the American Medical Informatics Association, Houston, TX; [Podium Abstract](https://www.dropbox.com/s/mccv9b10m4arvt3/2020%20AMIA%20Informatics%20Summit%20-%20Revision.pdf?dl=1)

______
### Getting Started

**Dependencies**
This repository is built using Python 3.6.2. To install the libraries used in this repository, run the line of code shown below from the within the project directory.
```
pip install -r requirements.txt
```

**Data**  
This code assumes that input data is stored in a GoogleSheet, thus this repository contains code which relies on 
Google's [DriveAPI](https://developers.google.com/drive/) and 
[SheetsAPI](https://developers.google.com/sheets/api/). In order to use this functionality you will need to:
- Complete the steps described [here](https://github.com/burnash/gspread)
- Save the json file containing your credentials to `./resources/programming/Google_API/` 
- Rename the credential file to "secret_client_gs.json"

This code assumes that your input Google Sheet will follow a specific format:

Phenotype | Cohort | Criteria | Phenotype_Criteria | Input_Type | Source_Domain | Source_Vocabulary | Source_Code | Source_Label
-- | -- | -- | -- | -- | -- | -- | -- | --
ADHD | Case | Include | Presence of at least 1 relevant code in >1 in-person visits, on separate calendar days | Code | Condition | ICD9CM | '314.0' | Attention deficit disorder of childhood
ADHD | Case | Include | Presence of  >1 prescriptions of ADHD-related medications | String | Drug | None | '%adderall%' | adderall




**SQL Queries**
- This project assumes that you will want to use the SQL queries that we have prepared and store as GitHub Gist. 
There are two types of queries run:
  1. Queries to map code sets
  2. Queries to create patient cohorts
