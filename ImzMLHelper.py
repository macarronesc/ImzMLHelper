from pyimzml.ImzMLParser import ImzMLParser


class ImzMLHelper:
    """
    The ImzMLHelper class provides some functionalities to help reading a imzML file stored
    in the cloud using the pyimzML library. We take advantage of range downloads to avoid
    downloading the whole file.

    Args:
        bucket (str): Storage bucket where the file is stored
        key (str): File key inside the bucket
        filename (str): Name that will be given to the local file
        s3: boto3 instance with access to the Object Storage
    """

    def __init__(self, bucket: str, key_imz: str, key_ibd: str, filename_imz: str, filename_ibd: str, s3):
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
        coordinates = parser.coordinates
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
        coordinates = parser.coordinates
        f.close()
        f_ibd.close()

    def split_imz(self):
        print()

    def get_all_coordinates(self):
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        coordinates = parser.coordinates
        f.close()
        return coordinates

    def get_metadata(self):
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)

        info = {"imzML filename": self.filename_imz, "ibd filename": self.filename_ibd,
                "Polarity": parser.polarity, "Int Group Id": parser.intGroupId,
                "Basic imzML metadata for reading spectra": parser.imzmldict}

        f.close()
        return info

    def get_intensity_info(self):
        global precision_aux
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)

        for key, value in parser.precisionDict.items():
            if parser.intensityPrecision == value:
                precision_aux = key

        info = {"Intensity Offsets": parser.intensityOffsets, "Intensity Precision": precision_aux,
                "Intensity Lengths": parser.intensityLengths}

        f.close()
        return info

    def get_intensity_info_point(self, coordinate: tuple):
        global precision_aux
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        position = parser.coordinates.index(coordinate)

        for key, value in parser.precisionDict.items():
            if parser.intensityPrecision == value:
                precision_aux = key

        info = {"Intensity Offsets": parser.intensityOffsets[position], "Intensity Precision": precision_aux,
                "Intensity Lengths": parser.intensityLengths[position]}

        f.close()
        return info

    def get_mz_info(self):
        global precision_aux
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)

        for key, value in parser.precisionDict.items():
            if parser.mzPrecision == value:
                precision_aux = key

        info = {"mz Offsets": parser.mzOffsets, "mz Group Id": parser.mzGroupId,
                "mz Precision": precision_aux, "mz Lengths": parser.mzLengths}
        f.close()
        return info

    def get_mz_info_point(self, coordinate: tuple):
        global precision_aux
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        position = parser.coordinates.index(coordinate)

        for key, value in parser.precisionDict.items():
            if parser.mzPrecision == value:
                precision_aux = key

        info = {"mz Offsets": parser.mzOffsets[position], "mz Group Id": parser.mzGroupId,
                "mz Precision": precision_aux, "mz Lengths": parser.mzLengths[position]}
        f.close()
        return info

    def get_point_data(self, coordinate: tuple):
        f = open(self.filename_imz, "r+b")
        f_ibd = open(self.filename_ibd, "r+b")
        parser = ImzMLParser(f, ibd_file=f_ibd)

        position = parser.coordinates.index(coordinate)

        info = parser.getspectrum(position)
        f.close()
        return info
