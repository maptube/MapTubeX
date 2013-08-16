__kernel void kmeans(__global float* inData,
	                 __global float* inCluster,
				     __global float* outDist2,
					 uint n,
					 uint d,
					 uint k
					 )
{
	//return the distance squared matrix so you can check the calculations on the CPU
	//inData=Data array (n*d)
	//inC=Centroid array (k*d)
	//outDS=Distance squared array (n*k)
	//n=num data points
	//d=dimensionality of data points
	//k=number of clusters

	//get the work item's unique id (data line)
	int idx=get_global_id(0); //this is the data line
	if (idx>=n) return; //needed for padding work items when number of rows isn't multiple of workgroup size
	//float* pValue = &inData[idx*d]; //optimise with autoinc pointers into inData and outDist2?
	
	//calculate distances of one data line (inData) to all cluster centres (inCluster)
	for (int i=0; i<k; i++) { //for every cluster
		float sum=0;
		for (int j=0; j<d; j++) { //for every data dimension
			//this is between 2 and 4 times faster than using pow(...)
			float delta = inData[idx*d+j]-inCluster[i*d+j];
			sum+=delta*delta;
			//sum+=pow(inData[idx*d+j]-inCluster[i*d+j],2);
		}
		//outDist2 is n*k matrix, so data line is indexed with idx*k
		outDist2[idx*k+i]=sum; //distance^2 from i cluster for this line
	}
}

__kernel void kmeans2(__global float* inData,
	                 __global float* inCluster,
				     __global int* outNearC,
					 uint n,
					 uint d,
					 uint k
					 )
{
	//return a list (one value per data point) of just the closest cluster index
	//inData=Data array (n*d)
	//inC=Centroid array (k*d)
	//outNearC=index of nearest cluster centre to this data point (n)
	//n=num data points
	//d=dimensionality of data points
	//k=number of clusters

	//get the work item's unique id (data line)
	int idx=get_global_id(0); //this is the data line
	if (idx>=n) return; //needed for padding work items when number of rows isn't multiple of workgroup size
	//float* pValue = &inData[idx*d]; //optimise with autoinc pointers into inData and outDist2?
	
	//calculate distances of one data line (inData) to all cluster centres (inCluster) to find closest cluster
	int minClusteri=0;
	float minClusterDist2=MAXFLOAT;
	for (int i=0; i<k; i++) { //for every cluster
		float sum=0;
		for (int j=0; j<d; j++) { //for every data dimension
			//this is between 2 and 4 times faster than using pow(...)
			float delta = inData[idx*d+j]-inCluster[i*d+j];
			sum+=delta*delta;
			//sum+=pow(inData[idx*d+j]-inCluster[i*d+j],2);
		}
		//outDist2 is n*k matrix, so data line is indexed with idx*k
		//outDist2[idx*k+i]=sum; //distance^2 from i cluster for this line
		if (sum<minClusterDist2) {
			minClusterDist2=sum;
			minClusteri=i;
		}
	}
	outNearC[idx]=minClusteri;
}

//in theory, you could calculate the new cluster centres on the GPU using a parallel mean algorithm
__kernel void parallelsum(__global float* outDist2,
	                      uint n,
						  uint d,
						  uint k)
{
	int idx=get_global_id(0);
}
