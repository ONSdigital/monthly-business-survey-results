# Estimation

## 1.0	Meta

*	Support area – Methodology – Sampling, Design and Estimation
*	Method theme – Estimation
*	Status - Combined ratio estimator implemented

## 2.0 Terminology

* stratified sampling – the process of breaking down a population into subsets called strata and sampling independently within each
* target variable - The variable of interest that requires data values to be estimated
* design weight - a survey weight that is the inverse of a unit's selection probability
* calibration factor - a sample dependent weight often referred to as a g weight
* auxiliary variable - The variable used as a predictor for a contributor's target variable under the model
* ratio estimation - A method of estimation that uses a ratio between a survey variable and an auxiliary variable to make estimates for non-sampled units
* combined ratio estimation - a form of ratio estimation that uses more than one stratum in each g weight band

## 3.0 Introduction

This specification covers ratio estimation.  The only sampling design supported is a stratified design with simple random sampling without replacement within each stratum.

Ratio estimation is commonly used in surveys of institutions such as businesses.  Stratification is usually used by size, because a small number of large units can provide a large contribution to the total, and there can be huge differences in size between big and small units.  Stratification by industry and/or region is also widely used.

All design-based estimation methods for such designs create a design weight, a-weight, using universe and sample counts. The proportion of businesses who are sampled or have a response are used to calculate a design weight.  In a strict sense, the design weight does not include any weighting for non-response, but sometimes the term “design weight” is used loosely to include this.

If the available auxiliary data is appropriate, ratio estimation is used; a calibration factor, g-weight, is calculated using auxiliary data and the a-weight. The calibration factor adjusts the sample for being unrepresentative in terms of the auxiliary variable. Note that the term calibration factor is used by Statistics Canada. Most literature refers to g-weights.

The strengths and limitations of the methods in this specification are as follows:

### Strengths

*	Relatively simple to use and check
*	Well known and widely used
*	Statistical properties well understood
*	Stratified random sampling widely used
*	Ratio estimation: Bias small for large enough sample

### Limitations

*	Requires random sampling
*	Requires appropriate sample design
*	Ratio estimation requires additional auxiliary data
*	Improper use of ratio estimation can lead to poor quality estimates – there must be a suitably strong positive linear relationship between the auxiliary data and the target variable

## 4.0 Assumptions and requirements

*	The design is assumed to be a stratified simple random sample without replacement
*	The auxiliary variable is a good predictor of the target variable
*	The files provided are assumed to be correct
*	Data to be in correct format (i.e. no symbol characters etc.)
*	Validation may need to occur before Estimation to avoid errors

## 5.0 Method inputs and outputs

### Method inputs

This method does not require the target variable which is being weighted as the weights do not calculate using any returns or imputed/constructed values.

The variables required are:

*	unique identifier: string – a unique identifier for each unit
*	period: string – period specified
*	strata: string – a stratum label
*	sample marker: Boolean – is the unit sampled?
*	auxiliary: numeric – auxiliary variable
*	calibration group: string – calibration group label


### Method outputs

*	period: string
*	cell: string – the stratum label
*	design weight: numeric – the final design weight
*	calibration factor

## 6.0 Method


**$i$** - an index denoting a single observation / business / row

**$h$** - an index denoting a stratum

**$j$** - an index denoting a calibration group (a collection of strata)

**$n_h$** or **$n_j$** - the sample size for stratum $h$ or calibration group $j$.  These values are inputs to the calculation of the _design weight_.

**$N_h$** or **$N_j$** - the population size for stratum $h$ or calibration group $j$. These values are inputs to the calculation of the _design weight_.

**$a_i$** is the _design weight_

**$g_i$** is the _calibration factor_

**$x_i$** is the _auxiliary variable_ for a given sampled _target value_

$$\begin{equation}
a_i=a_h=\frac{N_h}{n_h}\
\end{equation}$$

$$\begin{equation}
\text{sum of weighted auxiliary values} = \sum_{i=1}^{n_j} a_i x_i
\end{equation}$$

$$\begin{equation}
\text{sum of universe auxiliary values} = \sum_{i=1}^{N_j} x_i
\end{equation}$$

$$\begin{equation}
\text{calibration factor} = \frac{\text{sum of universe auxiliary values}}{\text{sum of weighted auxiliary values}}
\end{equation}$$
