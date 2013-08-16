//This really is vecadd in C++
//From the Heterogeneous Computing with OpenCL book P.36
//NOTE the post build step to copy the kernel file vec_add_kernel.cl to the output directory

#define __NO_STD_VECTOR //use cl::vector instead of STL one
#define __CL_ENABLE_EXCEPTIONS
#define CL_USE_DEPRECATED_OPENCL_1_1_APIS


//This is very naughty. The C++ wrapper forces the 1.2 OpenCL.dll functions for device retain/release, which don't exist in 1.1. This means you get DLL
//entry point errors if you use the C++ wrapper on a system that has the 1.1 dll. The solution is to either copy the 1.2 OpenCL.dll file into the executable
//directory, or force the C++ wrapper to use 1.1. This can be done in a kludge way by including cl.h, which defines CL_VERSION_1_2 and CL_VERSION_1_1, then
//undefine CL_VERSION_1_2 before including cl.hpp which would include cl.h again, except that it's already been included.
//ORDER OF FOLLOWING CRITICAL
#include <CL/cl.h>
#undef CL_VERSION_1_2
#include <CL/cl.hpp>
//END OF ORDER CRITICAL
#include <iostream>
#include <fstream>
#include <string>

int main() {
	const int N_ELEMENTS=1024;
	int *A=new int[N_ELEMENTS];
	int *B=new int[N_ELEMENTS];
	int *C=new int[N_ELEMENTS];

	for (int i=0; i<N_ELEMENTS; i++) {
		A[i]=i;
		B[i]=i;
	}

	try {
		//query for platforms
		cl::vector<cl::Platform> platforms;
		cl::Platform::get(&platforms);

		//get a list of devices on this platform
		cl::vector<cl::Device> devices;
		platforms[0].getDevices(CL_DEVICE_TYPE_GPU,&devices);

		//create a context for the devices
		cl::Context context(devices);

		//create a command queue for the first device
		cl::CommandQueue queue = cl::CommandQueue(context,devices[0]);

		//create memory buffers
		cl::Buffer bufferA=cl::Buffer(context,CL_MEM_READ_ONLY,N_ELEMENTS*sizeof(int));
		cl::Buffer bufferB=cl::Buffer(context,CL_MEM_READ_ONLY,N_ELEMENTS*sizeof(int));
		cl::Buffer bufferC=cl::Buffer(context,CL_MEM_WRITE_ONLY,N_ELEMENTS*sizeof(int));

		//copy the input data to the input buffers using the command queue for the first device
		queue.enqueueWriteBuffer(bufferA,CL_TRUE,0,N_ELEMENTS*sizeof(int),A);
		queue.enqueueWriteBuffer(bufferB,CL_TRUE,0,N_ELEMENTS*sizeof(int),B);

		//read the program source
		std::ifstream sourceFile("vector_add_kernel.cl");
		std::string sourceCode(std::istreambuf_iterator<char>(sourceFile),(std::istreambuf_iterator<char>()));
		cl::Program::Sources source(1,std::make_pair(sourceCode.c_str(),sourceCode.length()+1));

		//make program from source code
		cl::Program program=cl::Program(context,source);

		//build the program for the devices
		program.build(devices);

		//make kernel
		cl::Kernel vecadd_kernel(program,"vecadd");

		//set the kernel arguments
		vecadd_kernel.setArg(0,bufferA);
		vecadd_kernel.setArg(1,bufferB);
		vecadd_kernel.setArg(2,bufferC);

		//execute the kernel
		cl::NDRange global(N_ELEMENTS);
		cl::NDRange local(256);
		queue.enqueueNDRangeKernel(vecadd_kernel,cl::NullRange,global,local);

		//copy the output data back to the host
		queue.enqueueReadBuffer(bufferC,CL_TRUE,0,N_ELEMENTS*sizeof(int),C);

		//verify the result
		bool result=true;
		for (int i=0; i<N_ELEMENTS; i++) {
			if (C[i]!=A[i]+B[i]) {
				result=false;
				break;
			}
		}
		if (result)
			std::cout<<"Success"<<std::endl;
		else
			std::cout<<"Failed"<<std::endl;
	}
	catch (cl::Error error) {
		std::cout<<error.what()<<"("<<error.err()<<")"<<std::endl;
	}

	//there is no cleanup code in this example? added following...
	delete [] A;
	delete [] B;
	delete [] C;

	return 0;
}