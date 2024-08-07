## How to run
1. clone this branch
2. pip install git+https://github.com/ONSdigital/monthly-business-survey-results.git@v0.0.1
3. add paths in [json config](https://github.com/ONSdigital/monthly-business-survey-results/blob/testing_outputs/test_outputs_config.json)
4. run [testing_main](https://github.com/ONSdigital/monthly-business-survey-results/blob/480-testing-outputs/testing_main.py)

## Versioning notes
### Code
- use tags
- 0.0.1 for testing outputs
- 0.1.0 for taking SPP responses 
### Outputs
Name of file consists of:
- output name that reflects the contents of the file,
- code version tag,
- value to indicate version of the file created per code version
#### Output names
- imputation output
- estimation output
- winsorisation output
- standardising factors
- imputation links
