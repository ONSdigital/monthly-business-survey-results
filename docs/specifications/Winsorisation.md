# Winsorisation

## 1.0 Meta

* Support Area - Methodology – sample design & estimation
* Method Theme - Estimation
* Status - One-sided Winsorisation is implemented.
Two-sided Winsorisation is out of scope.

## 2.0 Terminology

* target variable - The variable of interest that requires data values to be winsorised
* design weight - a survey weight that is the inverse of a unit's selection probability
* auxiliary variable - The variable used as a predictor for a contributor's target variable under the model
* L-value - a bias parameter required for winsorisation
* calibration factor - a sample dependent weight often referred to as a g weight
* reference value - a unique identifier in each period
* outlier weight - the ratio of the treated to untreated target values - most are usually 1
* ratio estimation - A method of estimation that uses a ratio between a survey variable and an auxiliary variable to make estimates for non-sampled units.
* threshold - a target value above which the values are treated for a given unit
* predicted value - the expected value under the ratio model

## 3.0 Introduction

In business surveys, some responses can be very large and can distort
estimates when such a business is selected. Consequently, it is sometimes
desireable to reduce the effect of these businesses. This is known as outlier
treatment. This method applies a technique known as one-sided Winsorisation.
The objective of the method is to introduce a small bias, while reducing the
variance. This is intended to reduce the mean squared error of the total, a
measure of overall accuracy. The method works for stratified combined ratio 
estimation, commonly used in business surveys.

The method uses a pre-calculated parameter, 'L-value' that must be supplied
to the method, to calculate a threshold for each return.  The threshold calculated
depends upon the unit and variable involved.

If a target value is deemed influential, greater than the calculated threshold, an
outlier weight is calculated which will reduce the target value closer to the threshold.

Two-sided Winsorisation is out of scope for this specification.

## 4.0 Assumptions

* Data has been cleaned and appropriately processed (e.g using editing and/
or imputation) prior to the running of this method.
* An appropriate L-value is computed for each target variable based on the
source data and passed into the method.
* An appropriate design-weight is provided.
* Auxiliary and Calibration Factor values are provided.
* A unique reference value is provided to the method.
* Values from the period to be treated are organised into groups using
appropriate characteristics (e.g for business surveys, data from the same
industry sector, area or size may be considered a group)

## 5.0 Method Input and Output

### 5.1 Input Records

Input records must include the following fields:

* Reference - Any - a unique identifier, e.g Business Reporting Unit
* Period - String - for example "YYYY" or "YYYYMM", the period to be Winsorised
* Cell or Group - Any - represention of a stratum (e.g Standard
Industrial Classification by size)
* Target Variable - Numeric - the value to be treated
* Design Weight - Numeric - a supplied weight that reflects the sampling design
* Calibration Factor - Numeric - a weight that maintains the estimated calibration totals. 
* Auxiliary - Numeric - a secondary value used for prediction in ratio estimation. 


### 5.2 Output Records

Output records must include the the following fields of the correct types:

* Reference - Any - a unique identifier, e.g Business Reporting Unit
* Period - String - for example "YYYY" or "YYYYMM", the period to be Winsorised
* Outlier Weight - Numeric - A value between 0 and 1 which reflects the
reduction in a target variable due to Winsorisation

## 6.0 Method

### 6.1 Overall Method

The Winsorisation method performs the following steps to find outliers and
calculate outlier weights based on ratio estimation.

#### Assess Validity for Winsorisation

* A target value with a corresponding design weight of 1 is given an outlier weight of 1.

Target values considered for ratio estimation must
be checked to determine their eligibility for
Winsorisation.

* If design weight multiplied by calibration factor
is less than or equal to 1 the target value is not
Winsorised, the associated oultier weight is set to
1.

##### Ratio Estimation

For ratio estimation a _weight_ (calibration weight) is calculated from the
product of the supplied calibration factor and the design weight.

The product of the supplied design weight with the target value is used to calculate
a _weighted target value_.

A _weighted auxiliary value_ is calculated using the product of the design weight
and the supplied auxiliary value.

The sum of the _weighted target values_ divided by the sum of the _weighted
auxiliary values_ are used to calculate a
_weighted ratio_.

This _weighted ratio_ is then used to calculate a _predicted unit value_ associated
with the target value by multiplying the provided auxiliary by the computed
_weighted ratio_.

A _threshold_ is then calculated based on this _predicted unit value_, supplied L-value
and the _weight_ that was calculated from the calibration factor and design weight.

If the target value exceeds the _threshold_, then a modified value of the
target is calculated using the original value, _weight_ and _threshold_.

If the target value is less than or equal to the _threshold_
the target value remains unchanged.

An _outlier weight_ is calculated by using the _modified target value_ divided
by the original target value.
In the case of a target value that was not modified the
_outlier weight_ will be 1.

## 7.0 Calculations

**$i$** - an index denoting a single observation / business / row

**$h$** - an index denoting a stratum

**$j$** - an index denoting a calibration group (a collection of strata)

**$y_i$** - a target value.  This is an observed value for a given variable for
a given business.

**$n_h$** or **$n_j$** - the sample size for stratum $h$ or calibration group $j$.
These values are inputs to the calculation of the _design weight_ that is passed
to Winsorisation.  
**$N_h$** or **$N_j$** - the population size for stratum $h$ or calibration group
$j$. These values are inputs to the calculation of the _design weight_ that is
passed to Winsorisation.

**$w_i$** is the _weight_ which, for business $i$, is calculated as:
$w_i = a_i g_i$ when **ratio estimation** is used

**$L$** is the Winsorisation parameter supplied to the method - in ONS this is
calculated from historic data using a bespoke method based on the survey
requirements. Calculation of the L value is
beyond the scope of this specification.

**$a_i$** is the _design weight_ that is supplied to Winsorisation. The calculation
 of this is outside the scope of the
Winsorisation method itself.

**$g_i$** is the _calibration factor_ that is supplied to Winsorisation. The
calculation of this is outside the scope of the Winsorisation method itself.

**$x_i$** is the _auxiliary variable_ for a given sampled _target value_

**${\widehat{\mu}}_{i}$** is the _predicted unit value_ used in **ratio estimation**
and is calculated using the data to be Winsorised for each observation and the
sum of the _weighted target values_ divided by the sum of the _weighted auxiliary
values_ for each calibration group $j$ multiplied by the supplied auxiliary value

$$\begin{equation}
\hat{\mu}_i = \text{predicted unit value}
\end{equation}$$

<!-- markdownlint-disable MD013 -->
$$\begin{equation}
\text{predicted unit value} = x_i \frac{\text{sum of weighted target values}}{\text{sum of weighted auxiliary values}}
\end{equation}$$
<!-- markdownlint-enable MD013 -->

$$\begin{equation}
\text{sum of weighted target values} = \sum_{i=1}^{n_j} a_i y_i
\end{equation}$$

$$\begin{equation}
\text{sum of weighted auxiliary values} = \sum_{i=1}^{n_j} a_i x_i
\end{equation}$$

### Winsorisation

Once a target value has been identified for Winsorisation the outlier
weight, **$o_i$** is calculated as:

$$\begin{equation}
    o_i =
    \begin{cases}
1 & \text{if $y_i$  is not an outlier}\\
\frac{y_i^*}{y_i} & \text{if $y_i$ is an outlier}
    \end{cases}
 % for multiple cases you need to add \usepackage{amsmath}
\end{equation}$$

Where

$$\begin{equation}
y_i^* =
\begin{cases}
    y_i & \text{if $y_i$ is not an outlier, i.e. $y_i\leq k_i$}\\
    \frac{1}{w_i} y_i+(1- \frac{1}{w_i})k_i & \text{if $y_i$ is an outlier,
    i.e. $y_i>k_i$}
\end{cases}
\end{equation}$$

<!-- markdownlint-disable MD037 -->
and where $k_i$, the _threshold_ for Winsorisation is defined in the following section.
<!-- markdownlint-disable MD037 -->

### Threshold Calculation

#### Ratio Estimation

The unique threshold for a unit, $i$, is:

$$\begin{equation}
k_i= \hat{\mu}_i + \frac{L}{a_i g_i-1}
\end{equation}$$

Apart from parameter $L$, which is calculated with a bespoke
method using historic data, this is calculated using the data from the
period to be Winsorised.
