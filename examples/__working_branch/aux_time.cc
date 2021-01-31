/*
 * aux_time.cpp
 *
 *  Created on: Jul 3, 2012
 *      Author: Manos Athanassoulis
 *
 *
 *  You should have received a license file with this code: license.txt
 */


#include "aux_time.h"
#include <cmath>

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif


int my_clock_get_time(my_clock *clk)
{
	int ret = -1;
	
	#undef __MACH__ //to make sure we do not run this
	#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
		#warning "OS is MAC"
		clock_serv_t cclock;
		mach_timespec_t mts;
		host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
		ret=clock_get_time(cclock, &mts);
		mach_port_deallocate(mach_task_self(), cclock);
		clk->tv_sec = mts.tv_sec;
		clk->tv_nsec = mts.tv_nsec;
	#else
		#ifdef __USE_GETTIMEOFDAY
			ret=gettimeofday(clk,NULL);
		#else
			ret=clock_gettime(CLOCK_REALTIME, clk);
		#endif
	#endif
	
	return ret;

}

void my_print_clock(my_clock clk)
{
#ifdef __USE_GETTIMEOFDAY
    std::cout << "CLK: " << clk.tv_sec << " s + " << clk.tv_usec << " ns = " << ((double)clk.tv_sec+(double)clk.tv_usec/1000000) << " sec" << std::endl; 
#else
    std::cout << "CLK: " << clk.tv_sec << " s + " << clk.tv_nsec << " ns = " << ((double)clk.tv_sec+(double)clk.tv_nsec/1000000000) << " sec" << std::endl; 
#endif
}

long getclock_diff_us(my_clock clk1, my_clock clk2)
{
#ifdef __USE_GETTIMEOFDAY
	return 1000000*(clk2.tv_sec-clk1.tv_sec)+(clk2.tv_usec-clk1.tv_usec);
#else
	  return 1000000*(clk2.tv_sec-clk1.tv_sec)+(clk2.tv_nsec-clk1.tv_nsec)/1000;
#endif
}

long getclock_diff_ns(my_clock clk1, my_clock clk2)
{
#ifdef __USE_GETTIMEOFDAY
	return 1000000000*(clk2.tv_sec-clk1.tv_sec)+(clk2.tv_usec-clk1.tv_usec);
#else
	return 1000000000*(clk2.tv_sec-clk1.tv_sec)+(clk2.tv_nsec-clk1.tv_nsec);
#endif
}

double getclock_diff_s(my_clock clk1, my_clock clk2)
{
#ifdef __USE_GETTIMEOFDAY
	return (double)(clk2.tv_sec-clk1.tv_sec)+(double)(clk2.tv_usec-clk1.tv_usec)/1000000;
#else
	  return (double)(clk2.tv_sec-clk1.tv_sec)+(double)(clk2.tv_nsec-clk1.tv_nsec)/1000000000;
#endif
}
