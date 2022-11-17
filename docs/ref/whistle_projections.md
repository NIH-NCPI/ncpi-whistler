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

## Useful Whistle Documentation Links
[Whistle Language Reference](https://github.com/GoogleCloudPlatform/healthcare-data-harmonization/blob/master/mapping_language/doc/reference.md)
[Whistle Functions](https://github.com/GoogleCloudPlatform/healthcare-data-harmonization/blob/master/mapping_language/doc/builtins.md)
