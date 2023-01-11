<?php

namespace DreamFactory\Core\Rackspace\Components;

use DateTimeImmutable;
use InvalidArgumentException;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Config;

use DreamFactory\Core\Exceptions\DfException;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\NotImplementedException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\File\Components\RemoteFileSystem;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Core\Utility\FileUtilities;
use OpenStack\Common\Error\BadResponseError;
use OpenStack\Common\Error\BaseError;
use OpenStack\ObjectStore\v1\Service;
use OpenStack\ObjectStore\Resource\Container;
use OpenStack\ObjectStore\v1\Models\StorageObject;

use OpenStack\Identity\v2\Service as IdentityServiceV2;
use GuzzleHttp\Client;
use GuzzleHttp\Middleware as GuzzleMiddleware;
use OpenStack\Common\Transport\HandlerStack;
use OpenStack\Common\Transport\Utils;
use OpenStack\Common\Api\ApiInterface;

/**
 * Class OpenStackObjectStorageSystem
 *
 * @package DreamFactory\Core\Rackspace\Components
 */
class OpenStackObjectStorageSystem extends RemoteFileSystem
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var Service
     */
    protected $blobConn = null;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @throws DfException
     */
    protected function checkConnection()
    {
        if (empty($this->blobConn)) {
            throw new DfException('No valid connection to blob file storage.');
        }
    }

    /**
     * @param array $config
     *
     * @throws InvalidArgumentException
     * @throws DfException
     */
    public function __construct($config)
    {
        Session::replaceLookups($config, true);
        $this->container = trim(Arr::get($config, 'container'), '/');

        try {
            $os = $this->buildOpenStackClient($config);
            $options = $this->isRackspace($config['url']) ? ['catalogName' => "cloudFiles"] : [];
            $this->blobConn = $os->objectStoreV1($options);
            if (!$this->containerExists($this->container)) {
                $this->createContainer(['name' => $this->container]);
            }
        } catch (\InvalidArgumentException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            static::handleGuzzleException($ex);
            throw new DfException('Failed to launch OpenStack service: ' . $ex->getMessage());
        }       
    }

    /**
     * if apiKey is set
     *   use rackspace v2 with special "RAX-KSKEY:apiKeyCredentials"
     *      
     * else 
     *   case url ends with v2.0
     *     use v2 password authentication
     *   case url ends with v3
     *     use default (v3) authentication
     *   default 
     *     throw error Authentication URL must end with API version
     */
    private function buildOpenStackClient(array $config) : \OpenStack\OpenStack
    {
        if (empty($username = Arr::get($config, 'username'))) {
            throw new InvalidArgumentException('Object Store username can not be empty.');
        }
        // url and region were made required on Jul 23, 2015 by this commit 
        // https://github.com/dreamfactorysoftware/df-rackspace/commit/ec566e813e0a98f8dcc6a2ea9601c69c7eed973e
        // First release tag [0.0.1](https://github.com/dreamfactorysoftware/df-rackspace/releases/tag/0.0.1) 
        // created on Aug 7, 2015 includes config validation.
        // So support of optional Auth URL and Region for Rackspace is dropped.
        if (empty($authUrl = Arr::get($config, 'url'))) {
            throw new InvalidArgumentException('Object Store authentication URL can not be empty.');
        }
        if (empty($region = Arr::get($config, 'region'))) {
            throw new InvalidArgumentException('Object Store region can not be empty.');
        }
        $tenantName = Arr::get($config, 'tenant_name');
        
        $options = [
            'authUrl' => $authUrl,
            'region' => $region,
        ];
        $authOptions = null;
        $serviceBuilder = null;
        
        $apiKey = Arr::get($config, 'api_key');
        if (empty($apiKey)) {
            if (empty($password = Arr::get($config, 'password'))) {
                throw new InvalidArgumentException('Object Store credentials must contain an API key or a password.');
            }
            $version = $this->getAuthVersionFromUrl($authUrl);

            // https://docs.openstack.org/keystone/latest/contributor/http-api.html
            switch ($version) {
                case '3':
                    $authOptions = $this->buildAuthOptionsV3(
                        username: $username,
                        password: $password,
                        project: $tenantName
                    );
                    break;
                case '2.0':
                    $authOptions = [
                        'username' => $username,
                        'password' => $password,
                        'tenantName' => $tenantName,
                        'identityService' => $this->getIdentityServiceV2($options)
                    ];
                    break;
                default:
                    throw new InvalidArgumentException("Identity API v$version is not supported");
            }
        } else {
            $options['authUrl'] = $this->ensureV2InAuthUrl($authUrl);
            $authOptions = [
                'username' => $username,
                'apiKey' => $apiKey,
                'tenantName' => $tenantName,
                'identityService' => $this->getIdentityServiceV2($options, new ApiKeyAuthApi())
            ];
        }

        $options = array_merge($options, $authOptions);
        return new \OpenStack\OpenStack($options, $serviceBuilder);
    }

    /**
     * @return string version number without `'v'` prefix. i.e `'1.1'`, `'2.0'`, `'3'`
     * @throws \InvalidArgumentException if `$url` has no version at the end
     */
    private function getAuthVersionFromUrl(string $url) : string
    {
        preg_match(
            pattern: "/\/v(\d(\.\d)?)\/?$/i",
            subject: $url,
            matches: $matches
        );
        if (empty($matches)) {
            throw new InvalidArgumentException('URL must end with identity API version number');
        } else {
            $version = $matches[1][0];
            return $version;
        }        
    }

    private function buildAuthOptionsV3(
        string $username,
        string $password,
        string $project,
        string $domain = "default"
    ) {
        return [
            'user' => [
                'name' => $username,
                'password' => $password,
                'domain' => [
                    'id' => $domain
                ],
            ],
            'scope' => [
                'project' => [
                    'domain' => [
                        'id' => $domain
                    ],
                    'name' => $project
                ]
            ]
        ];
    }

    /**
     * {@see \OpenStack\OpenStack::getDefaultIdentityService}
     */
    private function getIdentityServiceV2(array $options, ApiInterface $api = null): IdentityServiceV2
    {
        $stack = HandlerStack::create();

        if (!empty($options['debugLog'])
            && !empty($options['logger'])
            && !empty($options['messageFormatter'])
        ) {
            $logMiddleware = GuzzleMiddleware::log($options['logger'], $options['messageFormatter']);
            $stack->push($logMiddleware, 'logger');
        }

        $clientOptions = [
            'base_uri' => Utils::normalizeUrl($options['authUrl']),
            'handler'  => $stack,
        ];

        if (isset($options['requestOptions'])) {
            $clientOptions = array_merge($options['requestOptions'], $clientOptions);
        }

        $client = new Client($clientOptions);        
        if (empty($api)) {
            return IdentityServiceV2::factory($client);
        } else {
            return new IdentityServiceV2($client, $api);
        }
    }

    private function isRackspace(string $url) : bool
    {
        return str_contains($url, 'identity.api.rackspacecloud.com');
    }

    private function ensureV2InAuthUrl(string $authUrl) : string
    {
        $pos = stripos($authUrl, '/v');
        if (false !== $pos) {
            $authUrl = substr($authUrl, 0, $pos);
        }
        return FileUtilities::fixFolderPath($authUrl) . 'v2.0';
    }
     
    /**
     * Object destructor
     */
    public function __destruct()
    {
        unset($this->blobConn);
    }

    /**
     * List all containers, just names if noted
     *
     * @param bool $include_properties If true, additional properties are retrieved
     *
     * @throws DfException
     * @return array
     */
    public function listContainers($include_properties = false)
    {
        // TODO listContainers seems to be not used
        $this->checkConnection();

        if (!empty($this->container)) {
            return $this->listResource($include_properties);
        }

        try {
            $containers = $this->blobConn->listContainers();
            $out = [];
            /** @var Container $container */
            foreach ($containers as $container) {
                $name = rtrim($container->name);
                $out[] = ['name' => $name, 'path' => $name];
            }

            return $out;
        } catch (\Exception $ex) {
            static::handleGuzzleException($ex);
            throw new DfException('Failed to list containers: ' . $ex->getMessage());
        }
    }

    /**
     * Gets all properties of a particular container, if options are false,
     * otherwise include content from the container
     *
     * @param string $container Container name
     * @param bool   $include_files
     * @param bool   $include_folders
     * @param bool   $full_tree
     *
     * @throws DfException
     * @return array
     */
    public function getContainer(
        $container,
        $include_files = true,
        $include_folders = true,
        $full_tree = false
    ){
        $this->checkConnection();
        $result = $this->getFolder($container, '', $include_files, $include_folders, $full_tree);

        return $result;
    }

    public function getContainerProperties($containerName)
    {
        $this->checkConnection();
        $result = ['name' => $containerName];

        try {
            $container = $this->blobConn->getContainer($containerName);
            $container->retrieve();
            $result['size'] = $container->bytesUsed;
        } catch (\Exception $ex) {
            static::handleGuzzleException($ex);
            throw new DfException('Failed to get container: ' . $ex->getMessage());
        }

        return $result;
    }

    /**
     * Check if a container exists
     *
     * @param  string $container Container name
     *
     * @throws DfException
     * @return boolean
     */
    public function containerExists($containerName = '')
    {
        $this->checkConnection();

        try {
            return $this->blobConn->containerExists($containerName);
        } catch (\Exception $ex) {
            static::handleGuzzleException($ex);
            throw new DfException('Failed to list containers: ' . $ex->getMessage());
        }
    }

    /**
     * @param array $properties
     * @param array $metadata
     *
     * @return array
     * @throws BadRequestException
     * @throws DfException
     * @throws \Exception
     */
    public function createContainer($properties, $metadata = [])
    {
        $this->checkConnection();

        $name = Arr::get($properties, 'name', Arr::get($properties, 'path'));
        if (empty($name)) {
            throw new BadRequestException('No name found for container in create request.');
        }
        try {
            $this->blobConn->createContainer(['name' => $name]);
            return ['name' => $name, 'path' => $name];
        } catch (BadResponseError $ex) {
            static::handleGuzzleException($ex);
            throw new DfException("Failed to create container '$name': " . $ex->getMessage());
        }
    }

    /**
     * Update of container properties not implemented
     *
     * @param string $container
     * @param array  $properties
     *
     * @throws DfException
     * @return void
     */
    public function updateContainerProperties($containerName, $properties = [])
    {
        throw new NotImplementedException('Update of container properties not implemented');
    }

    /**
     * Delete a container and all of its content
     *
     * @param string $container
     * @param bool   $force Force a delete if it is not empty
     *
     * @throws DfException
     * @throws \Exception
     * @return void
     */
    public function deleteContainer($containerName, $force = false): void
    {
        $this->checkConnection();
        try {
            $container = $this->blobConn->getContainer($containerName);

            $container->delete();
        } catch (\Exception $ex) {
            static::handleGuzzleException($ex);
            throw new DfException("Failed to delete container '$containerName': " . $ex->getMessage());
        }
    }

    /**
     * Check if a blob exists
     *
     * @param  string $container Container name
     * @param  string $name      Blob name
     *
     * @throws \Exception
     * @return boolean
     */
    public function blobExists($containerName = '', $name = '')
    {
        $this->checkConnection();
        $container = $this->blobConn->getContainer($containerName);
        return $container->objectExists($name);
    }

    /**
     * @param string $container
     * @param string $name
     * @param string $blob
     * @param string $type
     *
     * @throws DfException
     * @throws \Exception
     */
    public function putBlobData($containerName = '', $name = '', $blob = null, $type = '')
    {
        $this->checkConnection();
        try {
            $container = $this->blobConn->getContainer($containerName);
            $container->createObject([
                'name' => $name,
                'content' => $blob,
                'contentType' => $type
            ]);
        } catch (\Exception $ex) {
            static::handleGuzzleException($ex);
            throw new DfException("Failed to create blob '$name': " . $ex->getMessage());
        }
    }

    /**
     * @param string $container
     * @param string $name
     * @param string $localFileName
     * @param string $type
     *
     * @throws DfException
     * @throws \Exception
     */
    public function putBlobFromFile($containerName = '', $name = '', $localFileName = null, $type = '')
    {
        $this->checkConnection();
        try {
            $container = $this->blobConn->getContainer($containerName);

            $container->createObject(
                [
                    'name' => $name,
                    'content' => file_get_contents($localFileName),
                    'contentType' => $type
                ]
            );
        } catch (\Exception $ex) {
            static::handleGuzzleException($ex);
            throw new DfException("Failed to create blob '$name': " . $ex->getMessage());
        }
    }

    /**
     * @param string $container
     * @param string $name
     * @param string $src_container
     * @param string $src_name
     * @param array  $properties
     *
     * @throws DfException
     * @throws \Exception
     */
    public function copyBlob($containerName = '', $name = '', $srcContainerName = '', $src_name = '', $properties = [])
    {
        $this->checkConnection();
        try {
            $src_container = $this->blobConn->getContainer($srcContainerName);
            $source = $src_container->getObject($src_name);
            $source->copy([
                'destination' => $containerName . '/' . $name
            ]);
        } catch (\Exception $ex) {
            static::handleGuzzleException($ex);
            throw new DfException("Failed to copy blob '$name': " . $ex->getMessage());
        }
    }

    /**
     * Get blob
     *
     * @param  string $container     Container name
     * @param  string $name          Blob name
     * @param  string $localFileName Local file name to store downloaded blob
     *
     * @throws DfException
     * @throws \Exception
     */
    public function getBlobAsFile($containerName = '', $name = '', $localFileName = null)
    {
        //TODO this method is not called in OSS plan
        $this->checkConnection();
        try {
            $container = $this->blobConn->getContainer($containerName);
            $obj = $container->getObject($name);
            $obj->download();            
            // TODO implement me
            throw new NotImplementedException('getBlobAsFile not implemented for OpenStack');
        } catch (\Exception $ex) {
            static::handleGuzzleException($ex);
            throw new DfException("Failed to retrieve blob '$name': " . $ex->getMessage());
        }
    }

    /**
     * @param string $container
     * @param string $name
     *
     * @throws DfException
     * @throws \Exception
     * @return string
     */
    public function getBlobData($containerName = '', $name = '')
    {
        $this->checkConnection();
        try {
            $container = $this->blobConn->getContainer($containerName);
            $stream = $container->getObject($name)->download();            
            return $stream->getContents();
        } catch (\Exception $ex) {
            static::handleGuzzleException($ex);
            throw new DfException("Failed to retrieve blob '$name': " . $ex->getMessage());
        }
    }

    /**
     * @param string $container
     * @param string $name
     * @param bool   $noCheck
     *
     * @throws DfException
     * @throws \Exception
     */
    public function deleteBlob($containerName = '', $name = '', $noCheck = false)
    {
        $this->checkConnection();
        try {
            $container = $this->blobConn->getContainer($containerName);
            if (empty($container)) {
                throw new \Exception("No container named '$containerName'");
            }

            $obj = null;
            try {
                $obj = $container->getObject($name);
                $obj->delete();
            } catch (\Exception $ex) {
                if ($noCheck) {
                    return;
                }
                throw $ex;
            }
        } catch (\Exception $ex) {
            static::handleGuzzleException($ex);
            if ($ex instanceof BaseError) {
                throw new NotFoundException("File '$name' was not found.'");
            }
            throw new DfException('Failed to delete blob "' . $name . '": ' . $ex->getMessage());
        }
    }

    /**
     * List blobs
     *
     * @param  string $container Container name
     * @param  string $prefix    Optional. Filters the results to return only blobs whose name begins with the
     *                           specified prefix.
     * @param  string $delimiter Optional. Delimiter, i.e. '/', for specifying folder hierarchy
     *
     * @throws \Exception
     * @return array
     */
    public function listBlobs($containerName = '', $prefix = '', $delimiter = '')
    {
        $this->checkConnection();

        $options = [];
        if (!empty($prefix)) {
            $options['prefix'] = $prefix;
        }
        if (!empty($delimiter)) {
            $options['delimiter'] = $delimiter;
        }

        $container = $this->blobConn->getContainer($containerName);

        /** @var Collection $list */
        $list = $container->listObjects($options);

        $out = [];

        foreach ($list as $obj) {
            if ($obj->name === $prefix) { // this is requested "folder". skip it
                continue;
            } else {
                $out[] = $this->mapObjectToArray($obj);
            }
        }

        return $out;
    }

    /**
     * List blob
     *
     * @param  string $container Container name
     * @param  string $name      Blob name
     *
     * @throws DfException
     * @throws \Exception
     * @return array
     */
    public function getBlobProperties($containerName, $name)
    {
        $this->checkConnection();
        try {
            $obj = $this->retrieveObject($containerName, $name);
            if ($obj->name === null) {
                // Container itself here
                return [ 'name' => '.' ];
            } else {
                return $this->mapObjectToArray($obj);
            }
        } catch (DfException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new DfException('Failed to list metadata: ' . $ex->getMessage());
        }
    }

    private function mapObjectToArray(StorageObject $obj) : array
    {
        // Container::listObjects returns lastModified as DateTimeImmutable
        // but StorageObject::retrieve writes field from HTTP response header as string
        $lastModified = $obj->lastModified instanceof DateTimeImmutable
            ? $obj->lastModified->format('D, d M Y H:i:s \G\M\T')
            : $obj->lastModified;

        return [
            'name'           => $obj->name,
            'content_type'   => $obj->contentType,
            'content_length' => $obj->contentLength,
            'last_modified'  => $lastModified
        ];
    }

    protected function getBlobInChunks($containerName, $name, $chunkSize): \Generator
    {
        $obj = $this->retrieveObject($containerName, $name);
        $stream = $obj->download();
        while (!$stream->eof()) {
            yield $stream->read($chunkSize);
        }
    }

    /**
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \DreamFactory\Core\Exceptions\NotFoundException
     * @throws \DreamFactory\Core\Exceptions\RestException
     */
    private function retrieveObject(string $containerName, string $objectName)
    {
        try {
            $container = $this->blobConn->getContainer($containerName);
            $obj = $container->getObject($objectName);
            $obj->retrieve();
            return $obj;
        } catch (BadResponseError $ex) {
            $this->handleGuzzleException($ex);
        }
    }

    /**
     * @param $ex
     *
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \DreamFactory\Core\Exceptions\NotFoundException
     * @throws \DreamFactory\Core\Exceptions\RestException
     */
    private static function handleGuzzleException($ex)
    {
        if ($ex instanceof BadResponseError) {
            $code = $ex->getResponse()->getStatusCode();
            $message = static::buildShortErrorMessage($ex);
            switch ($code) {
                case 404:
                    throw new NotFoundException($message);
                case 400:
                    throw new BadRequestException($message);
                default:
                    throw new RestException($code, $message);
            }
        }
    }

    private static function buildShortErrorMessage(BadResponseError $ex)
    {
        // Laravel 6 message looks like "[404] Client error response. Not Found. https://storage101.dfw1.clouddrive.com/v1/MossoCloudFS_948417/dftest/path/file.txt"
        $request = $ex->getRequest();
        $method = $request->getMethod();
        $uri = $request->getUri();        

        $response = $ex->getResponse();
        $code = $response->getStatusCode();
        $phrase = $response->getReasonPhrase();
        $txId = Arr::first($response->getHeader('X-Trans-Id')); // alternative header X-Openstack-Request-Id

        return "[$code] Client error response. $phrase. $method $uri ($txId)";
    }
}
