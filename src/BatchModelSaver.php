<?php
    namespace unique\batchmodelsaver;

    use yii\db\ActiveRecord;
    use yii\db\Query;

    /**
     * Allows to easily do a batch insert for the models, which gives a huge speed boost, while still maintaining before, after event handling and validation.
     * Usage:
     * ```php```
     *     $saver = new BatchModelSaver();
     *
     *     $model = new Test();
     *     $model->data = '123';
     *     $saver->addToSaveList( $model );
     *
     *     $model = new Test();
     *     $model->data = '321';
     *     $saver->addToSaveList( $model );
     *
     *     $saver->commit();
     * ```php```
     *
     * Even models that are not new and need to be updated instead of created, can be saved this way. However updated models will not see any
     * speed benefits of batch saving and functionality is only provided for convenience.
     */
    class BatchModelSaver {

        /**
         * Models to be inserted to schema.
         * @var ActiveRecord[]
         */
        protected $create = [];

        /**
         * Models to be updated.
         * @var ActiveRecord[]
         */
        protected $update = [];

        /**
         * Should a transaction be used for the {@see commit()} operation.
         * @var bool
         */
        public $use_transaction_when_available = true;

        /**
         * Should table locks be used for {@see commit()} operation.
         * @var bool
         */
        public $use_table_locks = true;

        /**
         * Maximum rows to insert per one operation
         * @var int
         */
        public int $max_rows_to_insert = 1000;

        /**
         * Contains a list of tables to be locked with their lock type. Indexed by table name.
         * Tables are locked during {@see commit()} operation if {@see $use_table_locks} is true.
         * @var array
         */
        protected $locks = [];

        /**
         * Validates the model and calls beforeSave() method on it.
         * If everything passes, it is added to the list of models to be saved on {@see commit()} call.
         * @param ActiveRecord $model
         * @param bool $validate
         * @return bool True is returned if beforeSave() event returns true and validation is successful. Otherwise - false is returned.
         */
        public function addToSaveList( ActiveRecord $model, bool $validate = true ) {

            if ( $validate && !$model->validate() ) {

                return false;
            }

            if ( $model->isNewRecord ) {

                if ( $model->beforeSave( $model->isNewRecord ) ) {

                    $this->create[] = $model;
                } else {

                    return false;
                }
            } else {

                $this->update[] = $model;
            }

            $table_name = $model->tableName();
            if ( $this->use_table_locks && !isset( $this->locks[ $table_name ] ) ) {

                $this->locks[ $table_name ] = $table_name . ' WRITE';
            }

            return true;
        }

        /**
         * Commits all models added through {@see addToSaveList()} method to the database.
         * @throws \yii\db\Exception
         */
        public function commit() {

            $transaction = null;
            if ( $this->use_transaction_when_available && ( \Yii::$app->db->getTransaction() === null || \Yii::$app->db->getSchema()->supportsSavepoint() ) ) {

                $transaction = \Yii::$app->db->beginTransaction();
            }

            /**
             * Column names to be used on a batchInsert. Indexed by table names.
             */
            $columns = [];

            /**
             * Rows to be inserted using batchInsert. Indexed by table names.
             */
            $data = [];

            /**
             * Stores the name of the primary key and next value for each of the table
             * @var array{ key: string, value: int }[]
             */
            $primary_keys = [];

            /**
             * For every record to be inserted, stores attributes with their values, that are dirty and
             * will need to be updated on a model, once the insert is complete.
             * Indexed by the same indexes as {@see self::$create}
             */
            $update_insert_models = [];

            /**
             * Are the tables locked and need to be released?
             */
            $tables_locked = false;

            try {

                if ( $this->use_table_locks && $this->locks ) {

                    \Yii::$app->db->createCommand( 'LOCK TABLE ' . implode( ', ', $this->locks ) )
                        ->execute();
                    $tables_locked = true;
                }

                // Prepare the models to be inserted, by forming a $data entry for every table and setting the PKs.
                foreach ( $this->create as $index => $model ) {

                    $attributes = $model->getAttributes();
                    $table_name = $model->tableName();

                    if ( !isset( $columns[ $table_name ] ) ) {

                        $columns[ $table_name ] = array_keys( $attributes );

                        $metadata = \Yii::$app->db->createCommand( 'SHOW TABLE STATUS like \'' . $table_name . '\'' )
                            ->queryOne();

                        if ( $metadata['Auto_increment'] === null ) {

                            throw new \Exception( 'Unable to insert data to tables without auto_increment primary key.' );
                        } else {

                            // We cannot use here the value of Auto_increment, because it might be cached...

                            $key = reset( $keys );
                            $value = ( new Query() )
                                ->select( 'max(' . $key . ')' )
                                ->from( $table_name )
                                ->scalar();

                            $primary_keys[ $table_name ] = [
                                'key' => $key,
                                'value' => $value + 1,
                            ];
                        }
                    }

                    $update_insert_models[ $index ] = $model->getDirtyAttributes();

                    if ( !isset( $attributes[ $primary_keys[ $table_name ]['key'] ] ) ) {

                        $update_insert_models[ $index ][ $primary_keys[ $table_name ][ 'key' ] ] = $primary_keys[ $table_name ][ 'value' ];
                        $attributes[ $primary_keys[ $table_name ][ 'key' ] ] = $primary_keys[ $table_name ][ 'value' ]++;
                    }

                    $data[ $table_name ][] = array_values( $attributes );
                }

                foreach ( $columns as $table_name => $column_names ) {

                    do {

                        $batched_data = array_splice( $data[ $table_name ], 0, $this->max_rows_to_insert );

                        \Yii::$app->db->createCommand()
                            ->batchInsert( $table_name, $column_names, $batched_data )
                            ->execute();
                    } while ( count( $data[ $table_name ] ) > 0 );
                }

                if ( $this->use_table_locks && $tables_locked ) {

                    $tables_locked = false;
                    \Yii::$app->db->createCommand( 'UNLOCK TABLES' )
                        ->execute();
                }

                // No batch insert for update models
                foreach ( $this->update as $model ) {

                    if ( !$model->save( false ) ) {

                        throw new \Exception( 'Unable to update a model (id=' . $model->id . ', table=' . $model->tableName() . ')' );
                    }
                }

                /**
                 * Finish with created models. Mostly perform actions found in {@see \yii\db\ActiveRecord::insertInternal()} method.
                 */
                foreach ( $this->create as $index => $model ) {

                    $table_name = $model->tableName();
                    if ( isset( $update_insert_models[ $index ][ $primary_keys[ $table_name ][ 'key' ] ] ) ) {

                        $model->setAttribute( $primary_keys[ $model->tableName() ]['key'], $update_insert_models[ $index ][ $primary_keys[ $table_name ][ 'key' ] ] );
                    }

                    $changed_attributes = array_fill_keys( array_keys( $update_insert_models[ $index ] ), null );
                    $model->setOldAttributes( $update_insert_models[ $index ] );
                    $model->afterSave( true, $changed_attributes );
                }
            } catch ( \Throwable $error ) {

                if ( $transaction !== null ) {

                    $transaction->rollBack();
                }

                if ( $this->use_table_locks && $tables_locked ) {

                    \Yii::$app->db->createCommand( 'UNLOCK TABLES' )
                        ->execute();
                }

                throw $error;
            } finally {

                $this->create = [];
                $this->update = [];
                $this->locks = [];
            }

            if ( $transaction !== null ) {

                $transaction->commit();
            }
        }
    }