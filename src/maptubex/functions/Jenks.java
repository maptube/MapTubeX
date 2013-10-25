package maptubex.functions;

import java.util.ArrayList;
import java.util.Comparator;

public class Jenks {
	//https://stat.ethz.ch/pipermail/r-sig-geo/2006-March/000811.html
	//This is the one to copy:
	//https://github.com/tmcw/simple-statistics/blob/master/src/simple_statistics.js
	
	/**
     * @return int[]
     * @param list com.sun.java.util.collections.ArrayList
     * @param numclass int
     */
    public int[] getJenksBreaks(ArrayList list, int numclass) {
        //int numclass;
        int numdata = list.size();
        
        double[][] mat1 = new double[numdata + 1][numclass + 1];
        double[][] mat2 = new double[numdata + 1][numclass + 1];
        double[] st = new double[numdata];
        
        for (int i = 1; i <= numclass; i++) {
        	mat1[1][i] = 1;
        	mat2[1][i] = 0;
        	for (int j = 2; j <= numdata; j++)
        		mat2[j][i] = Double.MAX_VALUE;
        }
        double v = 0;
        for (int l = 2; l <= numdata; l++) {
        	double s1 = 0;
        	double s2 = 0;
        	double w = 0;
        	for (int m = 1; m <= l; m++) {
        		int i3 = l - m + 1;
        		
        		double val = ((Double)list.get(i3-1)).doubleValue();
        		
        		s2 += val * val;
        		s1 += val;
        		
        		w++;
        		v = s2 - (s1 * s1) / w;
        		int i4 = i3 - 1;
        		if (i4 != 0) {
        			for (int j = 2; j <= numclass; j++) {
        				if (mat2[l][j] >= (v + mat2[i4][j - 1])) {
        					mat1[l][j] = i3;
        					mat2[l][j] = v + mat2[i4][j - 1];
        				};
        			};
        		};
        	};
        	mat1[l][1] = 1;
        	mat2[l][1] = v;
        };
        int k = numdata;
        
        int[] kclass = new int[numclass];
        
        kclass[numclass - 1] = list.size() - 1;
        
        for (int j = numclass; j >= 2; j--) {
        	System.out.println("rank = " + mat1[k][j]);
        	int id =  (int) (mat1[k][j]) - 2;
        	System.out.println("val = " + list.get(id));
        	//System.out.println(mat2[k][j]);
        	
        	kclass[j - 2] = id;
        	
        	k = (int) mat1[k][j] - 1;
        };
        return kclass;
    }
    
    static class doubleComp implements Comparator {
    	public int compare(Object a, Object b) {
    		if (((Double) a).doubleValue() < ((Double)b).doubleValue())
    			return -1;
    		if (((Double) a).doubleValue() > ((Double)b).doubleValue())
    			return 1;
    		return 0;
    	}
    }
    
    /////////////////////////////////
    
    
    // ## Compute Matrices for Jenks
    //
    // Compute the matrices required for Jenks breaks. These matrices
    // can be used for any classing of data with `classes <= n_classes`
    /*double[][] jenksMatrices(double [] data, int n_classes) {
    	// in the original implementation, these matrices are referred to
    	// as `LC` and `OP`
    	//
    	// * lower_class_limits (LC): optimal lower class limits
    	// * variance_combinations (OP): optimal variance combinations for all classes
        int lower_class_limits[][]=new int[data.length][n_classes];
        double variance_combinations[][]=new double[data.length][n_classes];
        // loop counters
        int i, j;
        // the variance, as computed at each step in the calculation
        double variance = 0;
        
        // Initialize and fill each matrix with zeroes
        //for (i = 0; i < data.length + 1; i++) {
        //	var tmp1 = [], tmp2 = [];
        //	// despite these arrays having the same values, we need
        //	// to keep them separate so that changing one does not change
        //	// the other
        //	for (j = 0; j < n_classes + 1; j++) {
        //		tmp1.push(0);
        //		tmp2.push(0);
        //	}
        //	lower_class_limits.push(tmp1);
        //	variance_combinations.push(tmp2);
        //}

        //for (i = 1; i < n_classes + 1; i++) {
        //	lower_class_limits[1][i] = 1;
        //	variance_combinations[1][i] = 0;
        //	// in the original implementation, 9999999 is used but
        //	// since Javascript has `Infinity`, we use that.
        //	for (j = 2; j < data.length + 1; j++) {
        //		variance_combinations[j][i] = Infinity;
        //	}
        //}
        //My initialise and fill each matrix with zeroes
        for (i=0; i<data.length+1; i++) {
        	for (j=0; j<n_classes+1; j++) {
        		lower_class_limits[i][j]=0;
        		variance_combinations[i][j]=0;
        	}
        }
        for (i=1; i<n_classes+1; i++) {
        	lower_class_limits[1][i]=1;
        	variance_combinations[1][i]=0;
        	// in the original implementation, 9999999 is used but
        	// since Java has `Infinity`, we use that.
        	for (j = 2; j < data.length + 1; j++) {
        		variance_combinations[j][i] = Double.POSITIVE_INFINITY;
        	}
        }

        for (int l = 2; l < data.length + 1; l++) {

            // `SZ` originally. this is the sum of the values seen thus
            // far when calculating variance.
            double sum = 0;
            // `ZSQ` originally. the sum of squares of values seen
            // thus far
            double sum_squares = 0;
            // `WT` originally. This is the number of
            int w = 0;
            // `IV` originally
            int i4 = 0;
            
            // in several instances, you could say `Math.pow(x, 2)`
            // instead of `x * x`, but this is slower in some browsers
            // introduces an unnecessary concept.
            for (int m = 1; m < l + 1; m++) {
            	// `III` originally
            	int lower_class_limit = l - m + 1;
            	double val = data[lower_class_limit - 1];
            	
            	// here we're estimating variance for each potential classing
            	// of the data, for each potential number of classes. `w`
            	// is the number of data points considered so far.
            	w++;
            	
            	// increase the current sum and sum-of-squares
            	sum += val;
            	sum_squares += val * val;
            	
            	// the variance at this point in the sequence is the difference
            	// between the sum of squares and the total x 2, over the number
            	// of samples.
            	variance = sum_squares - (sum * sum) / w;
            	
            	i4 = lower_class_limit - 1;
            	
            	if (i4 != 0) {
            		for (j = 2; j < n_classes + 1; j++) {
            			// if adding this element to an existing class
            			// will increase its variance beyond the limit, break
            			// the class at this point, setting the `lower_class_limit`
            			// at this point.
            			if (variance_combinations[l][j] >=
            					(variance + variance_combinations[i4][j - 1])) {
            				lower_class_limits[l][j] = lower_class_limit;
            				variance_combinations[l][j] = variance +
            						variance_combinations[i4][j - 1];
            			}
            		}
            	}
            }
            
            lower_class_limits[l][1] = 1;
            variance_combinations[l][1] = variance;
        }
        
        // return the two matrices. for just providing breaks, only
        // `lower_class_limits` is needed, but variances can be useful to
        // evaluate goodness of fit.
        return {
        	lower_class_limits: lower_class_limits,
        	variance_combinations: variance_combinations
        };
    }*/
    
    // ## Pull Breaks Values for Jenks
    //
    // the second part of the jenks recipe: take the calculated matrices
    // and derive an array of n breaks.
    double[] jenksBreaks(double data[], int [][] lower_class_limits, int n_classes) {

        int k = data.length - 1;
        double kclass[] = new double[n_classes];
        int countNum = n_classes;
        
        // the calculation of classes will never include the upper and
        // lower bounds, so we need to explicitly set them
        kclass[n_classes] = data[data.length - 1];
        kclass[0] = data[0];
        
        // the lower_class_limits matrix is used as indexes into itself
        // here: the `k` variable is reused in each iteration.
        while (countNum > 1) {
        	kclass[countNum - 1] = data[lower_class_limits[k][countNum] - 2];
        	k = lower_class_limits[k][countNum] - 1;
        	countNum--;
        }
        
        return kclass;
    }

    // # [Jenks natural breaks optimization](http://en.wikipedia.org/wiki/Jenks_natural_breaks_optimization)
    //
    //
    // Depends on `jenksBreaks()` and `jenksMatrices()`
    /*double[] jenks(double [] data, int n_classes) {

        if (n_classes > data.length) return null;

        // sort data in numerical order, since this is expected
        // by the matrices function
        data = data.slice().sort(function (a, b) { return a - b; });

        // get our basic matrices
        var matrices = jenksMatrices(data, n_classes),
            // we only need lower class limits here
            lower_class_limits = matrices.lower_class_limits;

        // extract n_classes out of the computed matrices
        return jenksBreaks(data, lower_class_limits, n_classes);

    }*/


}
