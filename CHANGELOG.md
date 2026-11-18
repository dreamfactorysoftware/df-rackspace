# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [0.16.1] - 2026-11-18
- Laravel 13 compatibility; requires php-opencloud/openstack 3.16 or later.

## [0.14.1] - 2018-01-25
### Fixed
- DF-1285 Fixed error responses
### Changed
- Updated for latest version of rackspace sdk

## [0.14.0] - 2017-12-28
- Added package discovery
- DF-1150 Update copyright and support email
- Fixed current sdk usage of version check

## [0.13.0] - 2017-11-03
- Updated unit tests

## [0.12.0] - 2017-08-17
### Changed
- Reworked API doc usage and generation
- Set config-based cache prefix

## [0.11.0] - 2017-07-27
### Fixed
- DF-1144 Made DELETE behavior consistent across local and all remote file services

## [0.10.1] - 2017-06-08
### Fixed
- Rework config handling to distinquish rackspace and openstack better

## [0.10.0] - 2017-06-05
### Changed
- Cleanup - removal of php-utils dependency

## [0.9.0] - 2017-04-21
### Changed
- Use new service config handling for database configuration

## [0.8.0] - 2017-03-03
- Major restructuring to upgrade to Laravel 5.4 and be more dynamically available

## [0.7.0] - 2017-01-16
### Changed
- Dependency changes only

## [0.6.0] - 2016-11-17
- Dependency changes only

## [0.5.0] - 2016-10-03
### Changed
- DF-826 Protecting passwords.
- File service config rework from df-core.
- DF-641 Download files in chunks

## [0.4.0] - 2016-08-21
### Changed
- General cleanup from declaration changes in df-core for service doc and providers

## [0.3.1] - 2016-07-08
### Changed
- General cleanup from declaration changes in df-core.

## [0.3.0] - 2016-05-27
### Changed
- Moved seeding functionality to service provider to adhere to df-core changes.

## [0.2.0] - 2016-01-29
### Added

### Changed
- **MAJOR** Updated code base to use OpenAPI (fka Swagger) Specification 2.0 from 1.2

### Fixed

## 0.1.1 - 2016-01-05
### Fixed
- Correct typo in service_type name

## 0.1.0 - 2015-10-24
First official release working with the new [df-core](https://github.com/dreamfactorysoftware/df-core) library.

[Unreleased]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.14.1...HEAD
[0.14.1]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.14.0...0.14.1
[0.14.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.13.0...0.14.0
[0.13.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.12.0...0.13.0
[0.12.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.11.0...0.12.0
[0.11.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.10.1...0.11.0
[0.10.1]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.10.0...0.10.1
[0.10.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.9.0...0.10.0
[0.9.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.8.0...0.9.0
[0.8.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.7.0...0.8.0
[0.7.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.6.0...0.7.0
[0.6.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.5.0...0.6.0
[0.5.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.4.0...0.5.0
[0.4.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.3.1...0.4.0
[0.3.1]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.1.1...0.2.0
[0.1.1]: https://github.com/dreamfactorysoftware/df-rackspace/compare/0.1.0...0.1.1
