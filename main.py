import lithops
import ImzMLHelper

bucket = 'cloud-object-storage-r3-cos-standard-loq'
filekeyimz = 'Example_Processed.imzML'  # Example_Continuous
filekeyibd = 'Example_Processed.ibd'


def helper_test(aux, ibm_cos):
    helper = ImzMLHelper.ImzMLHelper(bucket, filekeyimz, filekeyibd, "test.imzML", "test.ibd", ibm_cos)
    helper.load_filename_imz()
    helper.load_filename_ibd()
    helper.test()
    # helper.test_ibd()
    print(helper.get_metadata())

    # print(len(helper.get_point_data(tuple((1, 2, 1)))[0]))

    print(helper.get_intensity_info())
    print(helper.get_intensity_info_point(tuple((1, 2, 1))))

    # print(helper.get_mz_info_point(tuple((1, 2, 1))))

    print(helper.get_all_coordinates())


if __name__ == '__main__':
    fexec = lithops.FunctionExecutor()
    fexec.map(helper_test, "")
    results = fexec.get_result()
