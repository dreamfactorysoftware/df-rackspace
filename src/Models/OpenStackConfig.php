<?php
namespace DreamFactory\Core\Rackspace\Models;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Models\BaseServiceConfigModel;

/**
 * Class RackspaceConfig
 *
 * @package DreamFactory\Core\Rackspace\Models
 */
class OpenStackConfig extends BaseServiceConfigModel
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
        'username'    => 'required',
        'password'    => 'required',
        'tenant_name' => 'required',
        'url'         => 'required',
        'region'      => 'required'
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
                if (('service_id' === $name) ||
                    'api_key' === $name ||
                    'storage_type' === $name ||
                    $column->autoIncrement
                ) {
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
            case 'password':
                $schema['description'] = 'The password for the service connection.';
                break;
            case 'tenant_name':
                $schema['description'] = 'Normally your account number.';
                break;
            case 'url':
                $schema['label'] = 'URL';
                $schema['description'] = 'The URL/endpoint for the service connection.';
                break;
            case 'region':
                $schema['description'] = 'The region to be accessed by this service connection.';
                break;
        }
    }

}