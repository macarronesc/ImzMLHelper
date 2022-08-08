import lithops
import ImzMLHelper

bucket = 'cloud-object-storage-r3-cos-standard-loq'
filekeyimz = 'Example_Continuous.imzML'  # Example_Continuous
filekeyibd = 'Example_Continuous.ibd'


def helper_test(aux, ibm_cos):
    helper = ImzMLHelper.ImzMLHelper(bucket, filekeyimz, filekeyibd, "test.imzML", "test.ibd", ibm_cos)
    helper.load_filename_imz()
    helper.load_filename_ibd()
    # helper.test()
    # helper.test_ibd()
    print(helper.get_intensity_info())
    print("\n")
    # print(helper.get_mz_info())
    print(helper.get_mz_info_point((1, 2, 1)))
    print("\n")
    print(helper.get_intensity_info_point((1, 2, 1)))
    print("\n")
    print(helper.get_data_point_local((1, 2, 1))[1])
    print(len(helper.get_data_point_local((1, 2, 1))[1]))
    print(len(helper.get_data_point_local((1, 2, 1))[1].tobytes()))
    print("\n")
    #print(helper.test())
    print(helper.get_data_point_cloud(tuple((1, 2, 1))))
    print(len(helper.get_data_point_cloud((1, 2, 1))))
    print(len(helper.get_data_point_cloud((1, 2, 1)).tobytes()))
    print("\n")
    print(helper.get_data_point_local((1, 2, 1))[1][12])
    print(helper.get_data_point_cloud(tuple((1, 2, 1)))[12])
    print(helper.get_data_point_local((1, 2, 1))[1][13])
    print(helper.get_data_point_cloud(tuple((1, 2, 1)))[13])
    print(helper.get_data_point_local((1, 2, 1))[1][8394])
    print(helper.get_data_point_cloud(tuple((1, 2, 1)))[8394])
    print(helper.get_data_point_local((1, 2, 1))[1][8389])
    print(helper.get_data_point_cloud(tuple((1, 2, 1)))[8389])


if __name__ == '__main__':
    fexec = lithops.FunctionExecutor()
    fexec.map(helper_test, "")
    results = fexec.get_result()
