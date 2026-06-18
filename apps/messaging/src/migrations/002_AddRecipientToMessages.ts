import { MigrationInterface, QueryRunner, TableColumn } from 'typeorm';

export class AddRecipientToMessages1640000000002 implements MigrationInterface {
  name = 'AddRecipientToMessages1640000000002';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.addColumn(
      'messages',
      new TableColumn({
        name: 'recipient',
        type: 'varchar',
        length: '255',
        isNullable: true,
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropColumn('messages', 'recipient');
  }
}
