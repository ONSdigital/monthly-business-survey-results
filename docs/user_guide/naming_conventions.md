# Naming conventions

## Methods

Methods can be generalised and have a different naming convention to their specific implementation. For example, for an imputation method we might define `target_variable` and `period` whereas when the method is implemented these might be replaced with `turnover` and `month` respectively.

|Name|Description|
|-|-|
|Reference | Single base unit of the dataset, typically a number assigned by the organisation to define a reporting unit |
|Population | All known references |
|Strata | Population split up by certain characteristics (e.g. SIC and employment group in MBS) for the purpose of sampling and estimation |
|Target variable | Variable to be imputed |
|Imputation | A method for estimating a response when a response to a question is missing (a.k.a. non-response) |
|Imputation class | A grouping of similar units where information can be drawn from to inform imputation. This is likely to be similar to strata but they serve a different purpose so they are different |
|Imputation link | A ratio of the sum of responses in one period to the sum of responses in the next period, where only references that responded in both periods are included in the sums | 
|Period | Date variable within a dataset |
|Auxiliary variable | A variable that is used to inform an estimate or model, e.g. a predictor for a reference's target variable, or as supplimentary information for estimation weights |
| SIC | Standard Industrial Classification is the classification system used for UK business |

## Code

|Name|Description|
|-|-|
|Imputation marker | A marker to describe the type of imputation used to generate an estimate for non-response, e.g. 'fir' is forward impute from a response |
|Imputation fill group | An ordered grouping of records containing a single reference and imputation class for consecutive periods of non-response. This variable is useful for many reasons, but its primary use is for creating cumulative imputation links for multiple periods of non-response.|
