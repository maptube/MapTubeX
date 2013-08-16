#define __NO_STD_VECTOR //use cl::vector instead of STL one
#define __CL_ENABLE_EXCEPTIONS
#define CL_USE_DEPRECATED_OPENCL_1_1_APIS

//ORDER OF FOLLOWING CRITICAL
#include <CL/cl.h>
#undef CL_VERSION_1_2
#include <CL/cl.hpp>
//END OF ORDER CRITICAL
#include <iostream>
#include <fstream>
#include <string>
//#include <stdio.h>
//#include <stdlib.h>
//#include <time.h> 
//#include <cstdlib>
#include <ctime>
#include <cmath>
#include <cfloat>

//function prototypes
void kmeans_cpu(float* inData, float* inCluster, int* outNearC, size_t n, size_t d, size_t k);

//BUFFER SIZE IS LIMITED!
//Query CL_DEVICE_MAX_MEM_ALLOC_SIZE to find out what it is.

//Main Program
int main() {
	try {
		std::cout<<CL_DEVICE_MAX_MEM_ALLOC_SIZE<<std::endl;
		//const unsigned int
		size_t k=4; //number of clusters to find
		size_t n=256*1000000; //1024; //number of data points (MUST BE MULTIPLE OF 256)
		size_t d=2; //dimensionality of each data point i.e. data[n][d] or n vectors of length d
		float *data=new float[n*d];
		float *centroid=new float[k*d];
		//float *dist2=new float[n*k]; //distance squared from each centroid
		int *clusterI=new int[n]; //index of closest cluster centroid for each point (i.e. which cluster does point belong to?)

		//make some random data
		//std::srand((unsigned int)std::time(0));
		std::srand(123456); //pick a fixed seed for consistency
		for (int i=0; i<n*d; i++) {
			data[i]=(float)std::rand()/(float)RAND_MAX;
			//std::cout<<"data="<<data[i]<<std::endl;
		}

		//pick initial cluster points - use first k points in the data for now - need to check Witten for what the best practice is
		for (int i=0; i<k*d; i++) {
			centroid[i]=data[i]; //this is really obtuse - both arrays laid out the same way, so only have to copy k vectors of length d
			//std::cout<<"centroid="<<centroid[i]<<std::endl;
		}

		//OpenCL part

		//query for platforms
		cl::vector<cl::Platform> platforms;
		cl::Platform::get(&platforms);

		//get a list of devices on this platform
		cl::vector<cl::Device> devices;
		platforms[0].getDevices(CL_DEVICE_TYPE_GPU,&devices);

		//create a context for the devices
		cl::Context context(devices);

		//create a command queue for the first device
		cl::CommandQueue queue = cl::CommandQueue(context,devices[0],CL_QUEUE_PROFILING_ENABLE); //PROFILING ENABLED

		//create memory buffers
		cl::Buffer bufferD=cl::Buffer(context,CL_MEM_READ_ONLY,n*d*sizeof(float)); //data buffer
		cl::Buffer bufferC=cl::Buffer(context,CL_MEM_READ_ONLY,k*d*sizeof(float)); //centroid buffer
		//cl::Buffer bufferDS=cl::Buffer(context,CL_MEM_WRITE_ONLY,n*k*sizeof(float)); //distance squared from centroids
		cl::Buffer bufferClusterI=cl::Buffer(context,CL_MEM_WRITE_ONLY,n*sizeof(int)); //index of closest cluster centroid

		//copy the input data to the input buffers using the command queue for the first device
		queue.enqueueWriteBuffer(bufferD,CL_TRUE,0,n*d*sizeof(float),data);
		queue.enqueueWriteBuffer(bufferC,CL_TRUE,0,d*k*sizeof(float),centroid);

		//read the program source
		std::ifstream sourceFile("kmeans_kernel.cl");
		std::string sourceCode(std::istreambuf_iterator<char>(sourceFile),(std::istreambuf_iterator<char>()));
		cl::Program::Sources source(1,std::make_pair(sourceCode.c_str(),sourceCode.length()+1));

		//make program from source code
		cl::Program program=cl::Program(context,source);

		//build the program for the devices
		program.build(devices);

		//make kernel
		//cl::Kernel vecadd_kernel(program,"kmeans");
		cl::Kernel vecadd_kernel(program,"kmeans2");

		//set the kernel arguments
		vecadd_kernel.setArg(0,bufferD);
		vecadd_kernel.setArg(1,bufferC);
		//vecadd_kernel.setArg(2,bufferDS); //kmeans
		vecadd_kernel.setArg(2,bufferClusterI); //kmeans2
		vecadd_kernel.setArg(3,n);
		vecadd_kernel.setArg(4,d);
		vecadd_kernel.setArg(5,k);

		//execute the kernel
		cl::NDRange global(n);
		cl::NDRange local(256);
		cl::Event timing_event; //perf
		//cl_int err_code; //perf
		queue.enqueueNDRangeKernel(vecadd_kernel,cl::NullRange,global,local,NULL,&timing_event);
		queue.finish();

		cl_ulong gpu_starttime;
		cl_ulong gpu_endtime;
		gpu_starttime = timing_event.getProfilingInfo<CL_PROFILING_COMMAND_START>();
		gpu_endtime = timing_event.getProfilingInfo<CL_PROFILING_COMMAND_END>();

		double gpu_ms = 1e-6 * (gpu_endtime-gpu_starttime); //not sure where the 1e-6 came from, but AMD used 1e-9 for seconds
		std::cout<<"GPU kmeans time="<<gpu_ms<<" milliseconds"<<std::endl;

		//copy the output data back to the host
		//queue.enqueueReadBuffer(bufferDS,CL_TRUE,0,n*k*sizeof(float),dist2); //kmeans
		queue.enqueueReadBuffer(bufferClusterI,CL_TRUE,0,n*sizeof(int),clusterI); //kmeans2

		//check the output - kmeans
		//for (int i=0; i<n; i++) { //loop through all data lines
		//	std::cout<<"dist2 i="<<i<<" : ";
		//	for (int c=0; c<k; c++) { //loop through all centroids and compare this line to each
		//		float sum=0;
		//		for (int j=0; j<d; j++) { //loop through all values on each data line (dimensionality of data points)
		//			sum+=pow(data[i*d+j]-centroid[c*d+j],2);
		//		}
		//		float gpu_value = dist2[i*k+c];
		//		float error = gpu_value-sum; //error between this data point and centroid c location
		//		std::cout<<error<<"  ";
		//	}
		//	std::cout<<std::endl;
		//}

		//Do a CPU version of kmeans to check the GPU data against
		int *cpu_clusterI=new int[n];
		LARGE_INTEGER frequency,counter1,counter2;
		QueryPerformanceFrequency(&frequency); //returns counts per second
		QueryPerformanceCounter(&counter1);
		kmeans_cpu(data,centroid,cpu_clusterI,n,d,k);
		QueryPerformanceCounter(&counter2);
		float t_ms=((float)(counter2.LowPart-counter1.LowPart))/(float)(frequency.LowPart)*1000; //milliseconds
		std::cout<<"CPU kmeans time="<<t_ms<<" milliseconds"<<std::endl;

		//check output - kmeans2
		bool result=true;
		for (int i=0; i<n; i++) { //loop through all data lines
			int gpu_index = clusterI[i]; //cluster index as calculated by the gpu
			int cpu_index = cpu_clusterI[i]; //cluster index as calculated by the cpu
			//std::cout<<"output i="<<i<<" : "<<gpu_index<<" "<<cpu_index<<std::endl;
			if (gpu_index!=cpu_index) {
				std::cout<<"Failed: "<<"output i="<<i<<" : "<<gpu_index<<" "<<cpu_index<<std::endl;
				result=false;
				break;
			}
		}
		if (result)
			std::cout<<"Success"<<std::endl;

		//and don't forget to clean up here
		delete [] data;
		delete [] centroid;
		//delete [] dist2;
		delete [] clusterI;
		delete [] cpu_clusterI;

	}
	catch (cl::Error error) {
		std::cout<<error.what()<<"("<<error.err()<<")"<<std::endl;
	}
	return 0;
}

/*
* KMeans code for CPU to use as a test of the GPU version.
* Basically a serial version of the parallel GPU kernel.
*
* inData=Data array (n*d)
* inC=Centroid array (k*d)
* outNearC=index of nearest cluster centre to this data point (n)
* n=num data points
* d=dimensionality of data points
* k=number of clusters
*
*/
void kmeans_cpu(float* inData, float* inCluster, int* outNearC, size_t n, size_t d, size_t k) {
	for (int idx=0; idx<n; idx++) //for every data point
	{
		int minClusteri=0;
		float minClusterDist2=FLT_MAX;
		for (int i=0; i<k; i++) //for every cluster
		{
			float sum=0;
			for (int j=0; j<d; j++)
			{
				float delta = inData[idx*d+j]-inCluster[i*d+j];
				sum+=delta*delta;
			}
			if (sum<minClusterDist2) {
				minClusterDist2=sum;
				minClusteri=i;
			}
		}
		outNearC[idx]=minClusteri;
	}
}

