## PheKnowVec
PheKnowVec is a novel method for deriving, implementing, and validating computational phenotypes. PheKnowVec leverages standardized clinical terminologies and open biomedical ontologies to derive, implement, and validate computational phenotype definitions in a scalable embedded structure.

Please see the [Project Wiki](https://github.com/callahantiff/PheKnowVec/wiki) for more information!

<br>

#### This is a Reproducible Research Repository
This repository contains more than just code, it provides a detailed and transparent narrative of our research process. For detailed information on how we use GitHub as a reproducible research platform, click [here](https://github.com/callahantiff/PheKnowVec/wiki/Using-GitHub-as-a-Reproducible-Research-Platform).

______
### Getting Started

**Dependencies**
This repository is built using Python 2.7.13. To install the libraries used in this repository, run the line of code shown below from the within the project directory.
```
pip install -r requirements.txt
```

**Data**
It's important to note that this repository makes heavy use of [PheKB](https://phekb.org). All of the phenotypes utilized in this project were initially developed in a GoogleSheet, thus this repository contains code which uses Google's [DriveAPI](https://developers.google.com/drive/) and[SheetsAPI](https://developers.google.com/sheets/api/).
