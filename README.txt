This is a pair of utilities which together can read a weighted interests 
profile and suggest some recommendations from a particular date that match it.

A sample weighted interests file is included.

Apps:

crawler.rb - crawls a days' worth of BBC programmes and puts them and 
their schedule into RDF using Jena TDB and Jruby.

recommend.rb - when given a profile, outputs matching items from the 
same Jena TDB as json

See INSTALL.txt for installation. It uses Java, Jruby and Jena TDB.

