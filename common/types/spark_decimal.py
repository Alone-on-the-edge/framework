from pyspark.sql.types import DataType, ByteType, ShortType, IntegerType, LongType, IntegralType, DecimalType

class SparkDecimalType:
    def __init__(self, decimal:DecimalType):
        self.precision = decimal.precision
        self.scale = decimal.scale

    @staticmethod
    def from_integral(dt: DataType):
        if isinstance(dt, ByteType):
            return DecimalType(3,0)
        elif isinstance(dt, ShortType):
            return DecimalType(5,0)
        elif isinstance(dt, IntegerType):
            return DecimalType(10,0)
        elif isinstance(dt, LongType):
            return DecimalType(19,0)
        else:
            raise TypeError("Datatype dt must of an IntegralType")
        
    def is_wider_than(self, other: DataType):
        if isinstance(other, DecimalType):
            return (self.precision - self.scale) >= (other.precision - other.scale) and self.scale >= other.scale
        elif isinstance(other, IntegerType):
            return self.is_wider_than(SparkDecimaltype.from_integral(other))
        else:
            return False
        
    def is_tighter_than(self, other: DataType):
        if isinstance(other, DecimalType):
            return (self.precision - self.scale) <= (other.precision - other.scale) and self.scale <= other.scale
        elif isinstance(other, IntegerType):
            return self.is_tighter_than(SparkDecimaltype.from_integral(other))
        else:
            return False
