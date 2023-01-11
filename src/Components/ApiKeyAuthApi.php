<?php

namespace DreamFactory\Core\Rackspace\Components;

use OpenStack\Common\Api\ApiInterface;

// https://docs.rackspace.com/support/how-to/getting-started-with-the-rackconnect-v30-api
class ApiKeyAuthApi implements ApiInterface
{
    public function postToken(): array
    {
        return [
            'method' => 'POST',
            'path'   => 'tokens',
            'params' => [
                'username' => [
                    'type'     => 'string',
                    'required' => true,
                    'path'     => 'auth.RAX-KSKEY:apiKeyCredentials',
                ],
                'apiKey' => [
                    'type'     => 'string',
                    'required' => true,
                    'path'     => 'auth.RAX-KSKEY:apiKeyCredentials',
                ],
                'tenantName' => [
                    'type' => 'string',
                    'path' => 'auth',
                ],
            ],
        ];
    }    
}
