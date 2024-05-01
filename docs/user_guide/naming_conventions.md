# Naming conventions

## Methods

Methods can be generalised and have a different naming convention to their specific implementation. For example, for an imputation method we might define `target_variable` and `period` whereas when the method is implemented these might be replaced with `turnover` and `month` respectively.

|Name|Description|
|-|-|
|Reference | Single base unit of the dataset |
|Population | All known references |
|Strata | Population split up by certain characteristics (e.g. SIC and employment group in MBS) |
|Target variable | Variable to be imputed |
|Period | Date variable for a dataset |
|Auxiliary variable | A variable that may be used as a predictor for a reference's target variable, or as supplimentary information for estimation weights |
