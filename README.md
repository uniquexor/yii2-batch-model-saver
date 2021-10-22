# yii2-batch-model-saver
Adds batch model saving functionality to yii2 framework.

Allows to easily do a batch insert for the models, which gives a huge speed boost, while still maintaining before, after event handling and validation.

## Instalation
This component requires php >= 7.4. To install it, you can use composer:
```
composer require unique/yii2-batch-model-saver "@dev"
```

## Usage:
<code>
    $saver = new BatchModelSaver();

    $model = new Test();
    $model->data = '123';
    $saver->addToSaveList( $model );

    $model = new Test();
    $model->data = '321';
    $saver->addToSaveList( $model );

    $saver->commit();
</code>
You can even update models that are not new. However, saving updated models will not yield any speed benefits of batch saving and functionality is only provided for convenience.