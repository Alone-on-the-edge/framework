# Instance method will contain atleast one argement(Mandatory), self
# With in the class to access INSTANCE VARIABLE,"self" keyword is required. outside class, access instance variable/methods using reference variable.

# OBJECT: Physical existence of a class

# "self": reference variable to current object. Using "self", we can access Instance variables and methods IN THE CLASS/CURRENT OBJECT
################################## START OF VARIABLES ##########################################
# Variables:
    static : Single copy. Recommended to call using Class name.
    non static/Instance Variables : Variables that vary from object to object. accesssed using "self" keyword.
    local : Variables declared inside instance methods.

# Methods:
    static
    non static
    class

#Converting Instance variables to dictionary or to check all the variables that belong to a certain object.
    s1 = Student("Aadya",1,100)
    print(s1.__dict__) #o/p {'name': "Aadya", 'rollno': 1,"Marks" : 100 }

#WHERE TO DECLARE INSTANCE VARIABLES
    Inside Constructor : 99%
    Inside instance method
    Outside of class using reference variable
    Examples:
      t1 = Test()
      t2 = Test()
      t1.x = 100 # If "x" exists inside instance method OR it is created as part of constructor, it will be overridden else a new INSTANCE VARIABLE will be created.   
    
    del self.variableName
    del referencevariable.variableName 

# WHERE TO DECLARE STATIC/CLASS VARIABLES    
    with in the class  -> 99%
    with in CONSTRUCTOR using CLASS NAME
    with in INSTANCE METHOD using CLASS NAME
    with in CLASS METHOD using "cls" variable OR CLASS NAME
    with in STATIC METHOD using CLASS NAME
    Outside the class using CLASS NAME

    # HOW TO ACCESS STATIC/CLASS VARIABLES
        with in the class  -> using "classname"
        with in CONSTRUCTOR using "self or classname"
        with in INSTANCE METHOD using "self or classname"
        with in CLASS METHOD using "cls or classname"
        with in STATIC METHOD using "classname"
        Outside the class using "classname"

    # HOW TO MODIFY STATIC/CLASS VARIABLES
        always use CLASS NAME to modify. using reference variable, it creates instance variable if variable doesnt exist, if it exists, it will update it to new value.

# WHERE TO DECLARE LOCAL VARIABLES
    These variables are specfic to methods only.

################################## END OF VARIABLES ##########################################

################################## START OF METHODS ##########################################
# INSTANCE METHOD(self) : Inside method body, if you are accesssing "Instance Variables", then that method has to be named as Instance method.

# CLASS METHOD(cls) : Inside method body, if you are accessing "CLASS/STATIC" variables using "cls". @classmethod is used to make a class as class method.

# STATIC METHOD (Neither self or cls) : General utility methods. If you are NOT USING CLASS OR INSTANCE VARIABLES, and using arguments to perform some work. @staticmethod is used to
#  make a method static. accessed using "class name"
################################## END OF METHODS ##########################################