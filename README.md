# openet
![test-result](https://github.com/aetriusgx/openet/actions/workflows/main.yaml/badge.svg)

OpenET research development (ReDev) is the software used in research conducted at Cal State Monterey Bay.

Started in June 2024 as an internship project, ReDev has become a full-fledged repository containing data and software used in various research projects.

Many things are ongoing, this repo tracks the following tasks:
- Ongoing research projects
- Developing data acquisition
- Improving project workflow
- Enhancing the safety and security of code

ReDev utilizes well-supported technology to conduct all research and software development much in part to Google Compute Engine and Earth Engine.

# Project Tracking
## Relevant Links
[Google Colab](https://colab.research.google.com/drive/1g8-hXfK--xd-Mnni3Mx-ONnVEBXC_hxH?usp=sharing)\
[NASA ARC-CREST](https://www.arc-crest.org/)

## Research Conducted
- [x] Forecast errors using Dynamic Time Warping
- [x] Measuring forecast improvement using snapping technique
- [x] Point vs Polygon forecasting comparison

## Research Ongoing
- [ ] Forecast errors using varying match windows for Dynamic Time Warping
- [ ] Forecast Reference ET (FRET) forecast error comparison to Dynamic Time Warping

# Peer-Reviewing
Peer reviewers should acknowledge that rerunning code from scratch will take hours and perhaps days to collect data.

## Recommened System Specifications
- x64 processor with at least 4 cores
- 16GB RAM
- 500GB usable storage

## Required Software
- Python 3.12+
- Jupyter Notebook | Jupyter Lab

## Running the code
A virtual environment is highly recommended to run this code. It will work using the global Python kernal but I highly advise against it.\
All pip packages needed are documented in `requirements.txt`.

### Copying the repository
```cmd
git clone https://github.com/aetriusgx/openet.git
```
### Creating the Python virtual environment
```
python -m venv openet | cd openet
```
### Running the environment and installing `pip` packages
```
source bin/Activate | pip install -r requirements.txt
```
