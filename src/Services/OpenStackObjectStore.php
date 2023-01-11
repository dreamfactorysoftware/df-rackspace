<?php
namespace DreamFactory\Core\Rackspace\Services;

use DreamFactory\Core\File\Services\RemoteFileService;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Rackspace\Components\OpenStackObjectStorageSystem;

use Illuminate\Support\Arr;

/**
 * Class OpenStackObjectStore
 *
 * @package DreamFactory\Core\Rackspace\Services
 */
class OpenStackObjectStore extends RemoteFileService
{
    /**
     * {@inheritdoc}
     */
    public function setDriver($config)
    {
        $this->container = trim(Arr::get($config, 'container'), ' /');

        if (empty($this->container)) {
            throw new InternalServerErrorException('Azure blob container not specified. Please check configuration for file service - ' .
                $this->name);
        }

        $this->driver = new OpenStackObjectStorageSystem($config);
    }
}