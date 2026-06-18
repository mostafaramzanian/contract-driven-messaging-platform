import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
} from 'typeorm';

@Entity('messages') // table name in the database
export class Message {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  title: string;

  @Column({ type: 'text' })
  content: string;

  @Column()
  sender: string;

  @Column({ nullable: true })
  recipient?: string;

  @CreateDateColumn()
  createdAt: Date;
}
