//Simple vector addition example from Heterogeneous Computing with OpenCL Book
//Strictly speaking, this is C, NOT C++, so the file should be vecadd.c

#include <stdio.h>
#include <stdlib.h>

//#include <cl.h>
#include <CL/cl.h>

const char* programSource =
	"__kernel                                      \n"
	"void vecadd(__global int *A,                  \n"
	"            __global int *B,                  \n"
	"            __global int *C)                  \n"
	"{                                             \n"
	"                                              \n"
	"  //get the work item's unique id             \n"
	"  int idx=get_global_id(0);                   \n"
	"                                              \n"
	"  // Add the corresponding locations of       \n"
	"  // 'A' and 'B' and store the result in 'C'. \n"
	"  C[idx]=A[idx]+B[idx];                       \n"
	"}"
	;

int main() {
	int *A=NULL; //in
	int *B=NULL; //in
	int *C=NULL; //out

	const int elements = 2048;
	size_t datasize=sizeof(int)*elements;

	//allocate space
	A=(int*)malloc(datasize);
	B=(int*)malloc(datasize);
	C=(int*)malloc(datasize);

	//initialise input data
	int i;
	for (i=0; i<elements; i++) {
		A[i]=i;
		B[i]=i;
	}

	//use this to check the output of each call
	cl_int status;

	//retrieve number of platforms
	cl_uint numPlatforms=0;
	status = clGetPlatformIDs(0,NULL,&numPlatforms);

	//allocate enough space for each platform
	cl_platform_id *platforms=NULL;
	platforms=(cl_platform_id*)malloc(numPlatforms*sizeof(cl_platform_id));

	//fill in the platforms
	status=clGetPlatformIDs(numPlatforms,platforms,NULL);

	//retrieve the number of devices
	cl_uint numDevices=0;
	status=clGetDeviceIDs(platforms[0],CL_DEVICE_TYPE_ALL,0,NULL,&numDevices);

	//allocate enough space for each device
	cl_device_id *devices;
	devices = (cl_device_id*)malloc(numDevices*sizeof(cl_device_id));

	//fill in the devices
	status = clGetDeviceIDs(platforms[0],CL_DEVICE_TYPE_ALL,numDevices,devices,NULL);

	//create a context and associate with devices
	cl_context context;
	context=clCreateContext(NULL,numDevices,devices,NULL,NULL,&status);

	//create a command queue and associate it with the device
	cl_command_queue cmdQueue;
	cmdQueue=clCreateCommandQueue(context,devices[0],0,&status);

	//create a buffer for data A
	cl_mem bufA;
	bufA=clCreateBuffer(context,CL_MEM_READ_ONLY,datasize,NULL,&status);

	//buffer B
	cl_mem bufB;
	bufB=clCreateBuffer(context,CL_MEM_READ_ONLY,datasize,NULL,&status);

	//buffer C - output data
	cl_mem bufC;
	bufC=clCreateBuffer(context,CL_MEM_WRITE_ONLY,datasize,NULL,&status);

	//write array A to the output buffer
	status=clEnqueueWriteBuffer(cmdQueue,bufA,CL_FALSE,0,datasize,A,0,NULL,NULL);

	//write array B
	status=clEnqueueWriteBuffer(cmdQueue,bufB,CL_FALSE,0,datasize,B,0,NULL,NULL);

	//create a program with source code
	cl_program program=clCreateProgramWithSource(context,1,(const char**)&programSource,NULL,&status);

	//build (compile) the program for the device
	status=clBuildProgram(program,numDevices,devices,NULL,NULL,NULL);

	//create the vector addition kernel
	cl_kernel kernel;
	kernel=clCreateKernel(program,"vecadd",&status);

	//associate the input and output buffers with the kernel
	status=clSetKernelArg(kernel,0,sizeof(cl_mem),&bufA);
	status=clSetKernelArg(kernel,1,sizeof(cl_mem),&bufB);
	status=clSetKernelArg(kernel,2,sizeof(cl_mem),&bufC);

	//Define an index space (global work size) of work
	//items for execution. A workgroup size (local work size)
	//is not required, but can be used.
	size_t globalWorkSize[1];

	//There are 'elements' work-items
	globalWorkSize[0]=elements;

	//Execute the kernel for execution
	status=clEnqueueNDRangeKernel(cmdQueue,kernel,1,NULL,globalWorkSize,NULL,0,NULL,NULL);

	//Read the device output buffer to the host array
	clEnqueueReadBuffer(cmdQueue,bufC,CL_TRUE,0,datasize,C,0,NULL,NULL);

	//verify the output
	int result=1;
	for(i=0; i<elements; i++) {
		if(C[i]!=i+i) {
			result=0;
			break;
		}
	}
	if (result) printf("output is correct");
	else printf("output is incorrect");

	//free opencl resources
	clReleaseKernel(kernel);
	clReleaseProgram(program);
	clReleaseCommandQueue(cmdQueue);
	clReleaseMemObject(bufA);
	clReleaseMemObject(bufB);
	clReleaseMemObject(bufC);
	clReleaseContext(context);

	//free host resources
	free(A);
	free(B);
	free(C);
	free(platforms);
	free(devices);

	return 0;
}