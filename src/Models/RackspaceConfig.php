<?php
namespace DreamFactory\Core\Rackspace\Models;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Models\BaseServiceConfigModel;

/**
 * Class RackspaceConfig
 *
 * @package DreamFactory\Core\Rackspace\Models
 */
class RackspaceConfig extends BaseServiceConfigModel
{
    protected $table = 'rackspace_config';

    protected $encrypted = ['password', 'api_key'];

    protected $protected = ['password'];

    protected $fillable = [
        'service_id',
        'username',
        'password',
        'tenant_name',
        'api_key',
        'url',
        'region',
    ];

    protected $rules = [
        'username'     => 'required',
        'tenant_name'  => 'required',
        'api_key'      => 'required',
        'url'          => 'required',
        'region'       => 'required'
    ];

    /**
     * {@inheritdoc}
     */
    public static function getConfigSchema()
    {
        $model = new static;

        $schema = $model->getTableSchema();
        if ($schema) {
            $out = [];
            foreach ($schema->columns as $name => $column) {
                /** @var ColumnSchema $column */
                if (('service_id' === $name) || 'password' === $name || 'storage_type' === $name || $column->autoIncrement) {
                    continue;
                }

                $temp = $column->toArray();
                static::prepareConfigSchemaField($temp);
                $out[] = $temp;
            }

            return $out;
        }

        return null;
    }

    /**
     * @param array $schema
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'username':
                $schema['description'] = 'The user name for the service connection.';
                break;
            case 'tenant_name':
                $schema['description'] = 'Normally your account number.';
                break;
            case 'api_key':
                $schema['label'] = 'API Key';
                $schema['description'] = 'The API key for the service connection.';
                break;
            case 'url':
                $schema['label'] = 'URL';
                $schema['description'] = 'The URL/endpoint for the service connection.';
                break;
            case 'region':
                $schema['type'] = 'picklist';
                // Cloud Files is a regionalized service. You can create your Cloud Files containers in any Rackspace data center
                // source https://docs.rackspace.com/docs/cloud-files/v1/general-api-info/service-access
                $schema['values'] = [ 
                    ['label' => 'Chicago', 'name' => 'ORD', 'url' => 'https://identity.api.rackspacecloud.com'],
                    ['label' => 'Dallas/Ft. Worth', 'name' => 'DFW', 'url' => 'https://identity.api.rackspacecloud.com'],
                    ['label' => 'London', 'name' => 'LON', 'url' => 'https://lon.identity.api.rackspacecloud.com'], 
                    ['label' => 'Hong Kong', 'name' => 'HKG', 'url' => 'https://identity.api.rackspacecloud.com'],
                    ['label' => 'Northern Virginia', 'name' => 'IAD', 'url' => 'https://identity.api.rackspacecloud.com'],
                    ['label' => 'Sydney', 'name' => 'SYD', 'url' => 'https://identity.api.rackspacecloud.com'],
                ];
                $schema['description'] = 'Select the region to be accessed by this service connection.';
                break;
        }
    }
}