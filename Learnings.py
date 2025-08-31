1. __str__ : It needs a return statement and not print.
2. __doc__ : Gives the doc string of the class
3. vars(): Shows instance variables and returns dict.
4. dir(): shows instance variables, methods, built-ins and returns list of string
5. class MyClass:
    pass
  
   type(MyClass) : <class 'type'>
   obj = MyClass()
   print(type(obj)): <class '__main__.MyClass'>. That means obj is an instance of MyClass
   
6.Generator Expression: A generator expression is like a list comprehension, but instead of building the entire list in memory, it creates a lazy iterator — which gives you one       item at a time.
       
   Perfect with sum, any, all, next
   Ex:  values = [1, 2, 3, 4, 5]

        # Sum of even numbers (lazy evaluation)
        total = sum(x for x in values if x % 2 == 0)
        print(total)  # 6

        # Check if any value > 10
        found = any(x > 10 for x in values)
    
    Analogy
        List comprehension: order all food now
        Generator expression: order only when you're ready to eat

7.  Function	Purpose	            Returns	            Default Support?
    getattr	Get value of attribute	  Value or default	    ✅ Yes
    hasattr	Check if attribute exists	True or False	    ❌ No
    setattr	Set/update attribute	    None	            ❌ N/A
    delattr	Delete attribute	        None (or error)	    ❌ No 

8. importing modules in python with and without __init__.py
Directory Structure

sor_onboarding/
└── metadata/
    ├── __init__.py
    ├── job_manager.py
    ├── job_names.py

| Without `__init__.py`                  | With `__init__.py`                |
| -------------------------------------- | --------------------------------- |
| `from metadata.job_manager import ...` | `from metadata import JobManager` |
| Harder to manage public interface      | Easy to expose a curated set      |
    