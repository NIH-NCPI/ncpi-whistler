# Writing Whistle Projections
There are two key components that must be passed to the whistle program related to whistle projection: mapping file and the projection directory. 

## mapping file
The mapping file is just the entry point for the whistle execution. This should live outside the projection directory. I typically keep mine in the study's root directory and the filename starts with an '_' character, but it can be kept anywhere other than the projection directory (if it is in the projection directory, whistle will complain about duplication functions being defined.)

An example of the mapping file might look like this:
```
$this: Transform_Dataset($root);
```
This basically tells Whistle to run the function, Transform_Dataset, passing the root of the data as the only parameter and return it as part of the current object. Then, we must define a function Transform_Dataset inside a file in the projection directory. 

## Projection Directory
Whistle projections are simply text files the implement Whistle code. Whistle will attempt to compile every file contained inside the specified directory, but will not iterate into subdirectories. To the whistle compiler, file extensions are irrelevant, but the convention is to suffix whistle code using the extension 'wstl'. 

Whistler employs a few additional conventions in the code it generates:
* General functions, such as those that create single components as opposed to whole resources are generally kept one per file, but for particularly interrelated functions two or three may sometimes be written to a single file. 
* These files will be named at the primary function's name, prefixed by an '_' character. 
* Resource producing functions, such as those created by buildsrcobs or buildsrcqr are named according to the type of resource they create, prefixed by 'wlib_dd_'. 
* All whistle code carries the extension, wstl.

## The Importance of the Meta.tag Property
All FHIR resources have a meta property which is used for a number of different purposes. One of the meta object's properties is the tag which is a list. Whistler uses this tag property to *tag* resources it loads so that it can catalog resources in a FHIR server more selectively. This catalog process is key to establishing referential integrity during loads and if the server has data from many studies, it could result in a lot of unnecessary load times (while it queries the FHIR server for a potentially large number of irrelevant resources). 

As a result, by default, Whistler will error out during the post-whistle sanity check if it encounters a resource without a meta.tag property. 

## FHIR Resources Require Whistler To Successfully Load
While most of the output from a Whistler run will be fully compliant with FHIR (assuming you wrote valid projections), any reference to a resource from the same dataset will be Whistler specific and will not validate correctly against a normal FHIR server. When Whistler loads resources, it will replace those special reference identifiers with valid references that point to the correct resource, or, if the resource can't be found in the target server, the resource containing that missing reference will be pushed to the back of the queue in the hope that the referenced resource will get loaded before it comes around once again. 

## Useful Whistle Documentation Links
[Whistle Language Reference](https://github.com/GoogleCloudPlatform/healthcare-data-harmonization/blob/master/mapping_language/doc/reference.md)
[Whistle Functions](https://github.com/GoogleCloudPlatform/healthcare-data-harmonization/blob/master/mapping_language/doc/builtins.md)
