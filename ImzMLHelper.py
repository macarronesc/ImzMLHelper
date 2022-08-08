from pyimzml.ImzMLParser import ImzMLParser
import numpy as np


class ImzMLHelper:
    """
    The ImzMLHelper class provides some functionalities to help reading a imzML file stored
    in the cloud using the pyimzML library. We take advantage of range downloads to avoid
    downloading the whole file.

    Args:
        bucket (str): Storage bucket where the file is stored
        key_imz (str): Imz file key inside the bucket
        key_ibd (str): Ibd file key inside the bucket
        filename (str): Name that will be given to the local file
        s3: boto3 instance with access to the Object Storage
    """

    def __init__(self, bucket: str, key_imz: str, key_ibd: str, filename_imz: str, filename_ibd: str, s3):
        """

        :param bucket:
        :param key_imz:
        :param key_ibd:
        :param filename_imz:
        :param filename_ibd:
        :param s3:
        """
        self.bucket = bucket

        self.key_imz = key_imz
        self.key_ibd = key_ibd

        self.filename_imz = filename_imz
        self.filename_ibd = filename_ibd

        self.ibm_cos = s3

    def load_filename_imz(self):
        """Download the file imz and store it in a local file. This function should be one of the first
        to be called.
        """
        self.ibm_cos.download_file(self.bucket, self.key_imz, self.filename_imz)

    def load_filename_ibd(self):
        self.ibm_cos.download_file(self.bucket, self.key_ibd, self.filename_ibd)

    def test(self):
        print(self.filename_imz)
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        # print(parser.spectrum_full_metadata)
        # print(parser.get_spectrum_as_string(1))
        print(parser.filename)
        print(parser.root)
        print(parser.sizeDict)
        print(parser.metadata)
        print(parser.polarity)
        print(parser.intGroupId)
        print(parser.imzmldict)
        print(parser.iterparse)
        print(parser.precisionDict)
        f.close()

    def test_ibd(self):
        print(self.filename_imz)
        print(self.filename_ibd)
        f = open(self.filename_imz, "r+b")
        f_ibd = open(self.filename_ibd, "r+b")
        parser = ImzMLParser(f, ibd_file=f_ibd)
        # print(parser.get_spectrum_as_string(1))
        print("\n")
        print("\n")
        print(parser.get_physical_coordinates(3))
        print("\n")
        print("\n")
        print(parser.getspectrum(3))
        print("\n")
        print("\n")
        # print(parser.get_spectrum_as_string(1))
        print(parser.polarity)
        print(parser.intGroupId)
        print(parser.imzmldict)
        print(parser.iterparse)
        print(parser.precisionDict)
        f.close()
        f_ibd.close()

    def split_imz(self):
        f = open(self.filename_imz, "r+b")
        f_ibd = open(self.filename_ibd, "r+b")
        parser = ImzMLParser(f, ibd_file=f_ibd)
        data = []

        for i in range(len(parser.coordinates)):
            data.append(parser.getspectrum(i))

        f.close()
        f_ibd.close()
        return data

    def get_all_coordinates(self):
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        coordinates = parser.coordinates
        f.close()
        return coordinates

    def get_metadata(self):
        print(self.filename_imz)
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)

        info = {"imzML filename": self.filename_imz, "ibd filename": self.filename_ibd,
                "Polarity": parser.polarity, "Int Group Id": parser.intGroupId,
                "Basic imzML metadata for reading spectra": parser.imzmldict}

        f.close()
        return info

    def get_intensity_info(self):
        precision_aux_intensity = 0
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)

        for key, value in parser.precisionDict.items():
            if parser.intensityPrecision == value:
                precision_aux_intensity = key

        info = {"Intensity Offsets": parser.intensityOffsets, "Intensity Precision": precision_aux_intensity,
                "Intensity Lengths": parser.intensityLengths}

        f.close()
        return info

    def get_intensity_info_point(self, coordinate: tuple):
        precision_aux_intensity_point = 0
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        position = parser.coordinates.index(coordinate)

        for key, value in parser.precisionDict.items():
            if parser.intensityPrecision == value:
                precision_aux_intensity_point = key

        info = {"Intensity Offsets": parser.intensityOffsets[position], "Intensity Precision":
            precision_aux_intensity_point, "Intensity Lengths": parser.intensityLengths[position]}

        f.close()
        return info

    def get_mz_info(self):
        precision_aux_mz = 0
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)

        for key, value in parser.precisionDict.items():
            if parser.mzPrecision == value:
                precision_aux_mz = key

        info = {"mz Offsets": parser.mzOffsets, "mz Group Id": parser.mzGroupId,
                "mz Precision": precision_aux_mz, "mz Lengths": parser.mzLengths}
        f.close()
        return info

    def get_mz_info_point(self, coordinate: tuple):
        precision_aux_mz_point = 0
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        position = parser.coordinates.index(coordinate)

        for key, value in parser.precisionDict.items():
            if parser.mzPrecision == value:
                precision_aux_mz_point = key

        info = {"mz Offsets": parser.mzOffsets[position], "mz Group Id": parser.mzGroupId,
                "mz Precision": precision_aux_mz_point, "mz Lengths": parser.mzLengths[position]}
        f.close()
        return info

    def get_data_point_local(self, coordinate: tuple):
        f = open(self.filename_imz, "r+b")
        f_ibd = open(self.filename_ibd, "r+b")
        parser = ImzMLParser(f, ibd_file=f_ibd)

        position = parser.coordinates.index(coordinate)

        info = parser.getspectrum(position)
        f.close()
        f_ibd.close()
        return info

    # TO-DO
    # Se puede retornar los datos en bytes brutos
    def get_data_point_cloud(self, coordinate: tuple):
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)

        position = parser.coordinates.index(coordinate)
        intensityOffset = parser.intensityOffsets[position]
        intensitySize = (parser.sizeDict.get(parser.intensityPrecision) * parser.intensityLengths[position]) - 1
        # intensitySize = parser.intensityLengths[position]

        """header_request = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key_ibd,
                                                 Range='bytes={}-{}'.format(0, 16))
        body = header_request['Body']
        f_ibd = open(self.filename_ibd, "w+b")
        for i in body:
            f_ibd.write(i)
        f_ibd.close()"""

        header_request = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key_ibd,
                                                 Range='bytes={}-{}'.format(intensityOffset,
                                                                            intensityOffset + intensitySize))
        body = header_request['Body']
        f_ibd = open(self.filename_ibd, "a+b")
        array = []
        for i in body:
            array = np.append(array, np.frombuffer(i, dtype=parser.intensityPrecision))
            f_ibd.write(i)
        f_ibd.close()

        return array
