# FHIR Hosts
The FHIR *hosts* file defines FHIR Servers that can may be used as targets for NCPI Whister to load data into. This file is a simply YAML file whose root entries define the alias for each potential target destination. Each entry will have an **auth_type** property which determines which type of authentication the mechanism will be used during the load process. Different auth_types have different properties that must be configured, but there are some properties that are common to all entries. 

The file should be named, *fhir_hosts*, and reside in the current working directory for any given run. 

## Example Host Entry
```
dev:
    auth_type: 'auth_basic'
    username: 'someuser'
    password: 'somepassword'
    host_desc: 'Local Server'
    target_service_url: 'http://localhost:8000'
```

In the example above, we define a host that will be recognized by NCPI Whistler as **dev** which uses basic_auth. Because it uses basic_auth, we are expected to provide a *username* and a *password*. There is also a descriptive property, *host_desc*, which may be displayed during execution to remind the user which server is currently the target. 

Finally, there is the property, *target_service_url*, which is the url associated with the root FHIR api. 

## Universal Properties
The following properties are common for all authentication types. 

### auth_type (required)
This property is indicates which authentication type is to be used. It must match one of the valid auth types listed below. 

### host_desc 
This is just a friendly description suitable for display during runtime to indicate which server is in use. 

### target_service_url (required)
This is the url associated with the root FHIR api. This must be correct. All REST endpoints will be constructed with this string as the base. 

## Auth Types
At this time, there are four auth types that can be used. It is easy to add additional auth types if one knows python. It is just a matter of adding a new entry to the [ncpi-fhir-client](https://github.com/NIH-NCPI/ncpi-fhir-client/tree/main/ncpi_fhir_client/fhir_auth). Documentation on this is yet to come, but the interface is hopefully pretty straightforward. 

### auth_basic
This is just basic auth with a password. The passwords are simply added into the API calls according to the HTTP basic auth standard.

Relevant Properties
#### username
This is the username used for authentication

#### password
This is the password to be incorporated as part of the authenticated request

### GCP Target Service
This approach is allows NCPI Whistler to load data into the GCP endpoint using a service account token JSON file, which can be generated using the GCP CLI or through the GCP console online. 

Relevant Properties
#### service_account_token
This is the full path to the service account token. While this file doesn't have to live within the current working directory, if you were to use the fully dockerized version of NCPI Whistler, anything that doesn't exist within the current working directory is inaccessible. 

### auth_gcp_oath2
This auth type employees google's OpenAuth2 protocol. Because this system runs within a command-line environment as opposed to being directly integrated with the web, it is a bit messier to use. The first attempt to make the connection will result in a prompt with a web URI which must be accessed with a web browser, with the user granting permission to use the services on their behalf. The response to the approval must then be copied back into the running terminal session so that it can continue. 

Relevant Properties
#### oa2_client_token
This is the path to the token created by GCP. Concerns about where the file can be are the same as with service account tokens. 

### auth_kf_aws
This auth type employs the authorization scheme used by the Kids First team which involves a cookie acquired by the keycloak service and an optional username and password. 

Relevant Properties
#### cookie
This is the contents of the session cookie that was created during a recent visit to the FHIR service. 

#### username
This is the username used for authentication

#### password
This is the password to be incorporated as part of the authenticated request

# Template fhir_hosts File Generation
NCPI Whistler's play script will dump an example fhir_hosts file content to std::out when it is run from a directory where no hosts file is found. To save this to a file for quick editing run the following command:

```
play > fhir_hosts
```

This will redirect that output into a fhir containing an example containing an entry from each different auth_type currently available. The user can then tweak the entries for the types of auth they need and create as many hosts as are needed.

Please note that the command above will overwrite any file in the current working directory named, *fhir_hosts*, so only run that command if you don't already have a hosts file that contains changes specific to your needs. 

# Security Notice
Because the hosts file may contain sensitive information such as usernames and passwords, it is **strongly recommended** that these files *not* be checked into any version control (VC) system. It is best for the user to add fhir_hosts to any ignore list or VC black list system such as the file, .gitignore. Also, if you are using the fully dockerized version of Whistler and must keep token details within the working directory, it is **strongly recommended** that you store those in a subdirectory that is excluded from version control or hack the play docker stubs to correctly map those directories into the image. 